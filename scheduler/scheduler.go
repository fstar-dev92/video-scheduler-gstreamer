package scheduler

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-gst/go-glib/glib"
	"github.com/go-gst/go-gst/gst"
)

// StreamItem represents a scheduled video item
type StreamItem struct {
	Type     string        // "file" or "test" (test pattern)
	Source   string        // File path for "file" type
	Start    time.Time     // When to start playing this item
	Duration time.Duration // How long to play this item
}

// StreamScheduler manages a GStreamer pipeline for scheduled playback
type StreamScheduler struct {
	host      string
	port      int
	items     []StreamItem
	pipeline  *gst.Pipeline
	mainLoop  *glib.MainLoop
	vselector *gst.Element
	aselector *gst.Element
	mutex     sync.Mutex
	stopChan  chan struct{}
	running   bool
	sources   map[int][]*gst.Element // Track sources by index
}

// NewStreamScheduler creates a new stream scheduler
func NewStreamScheduler(host string, port int) (*StreamScheduler, error) {
	return &StreamScheduler{
		host:     host,
		port:     port,
		items:    make([]StreamItem, 0),
		stopChan: make(chan struct{}),
		sources:  make(map[int][]*gst.Element),
	}, nil
}

// AddItem adds a scheduled item to play
func (s *StreamScheduler) AddItem(item StreamItem) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.items = append(s.items, item)
}

// Start begins the scheduling and streaming
func (s *StreamScheduler) Start() error {
	s.mutex.Lock()
	if s.running {
		s.mutex.Unlock()
		return fmt.Errorf("scheduler is already running")
	}
	s.running = true
	s.mutex.Unlock()

	// Create the pipeline
	var err error
	s.pipeline, err = gst.NewPipeline("streaming-pipeline")
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %v", err)
	}

	// Create elements for output (RTP streaming)
	s.vselector, err = gst.NewElement("input-selector")
	if err != nil {
		return fmt.Errorf("failed to create vselector: %v", err)
	}
	s.vselector.SetProperty("name", "vselector")

	s.aselector, err = gst.NewElement("input-selector")
	if err != nil {
		return fmt.Errorf("failed to create aselector: %v", err)
	}
	s.aselector.SetProperty("name", "aselector")

	videoconv, err := gst.NewElement("videoconvert")
	if err != nil {
		return fmt.Errorf("failed to create videoconv: %v", err)
	}

	// Add a videoscale element to handle resolution
	videoscale, err := gst.NewElement("videoscale")
	if err != nil {
		return fmt.Errorf("failed to create videoscale: %v", err)
	}

	// Add capsfilter to limit video size and framerate
	videocaps, err := gst.NewElement("capsfilter")
	if err != nil {
		return fmt.Errorf("failed to create videocaps: %v", err)
	}

	// Set video to 720p max resolution at 30fps with more flexible format settings
	capstr := "video/x-raw,width=(int)[1,1920],height=(int)[1,1080],framerate=(fraction)[1/1,30/1]"
	caps := gst.NewCapsFromString(capstr)
	videocaps.SetProperty("caps", caps)

	// Change to h264 encoder with more compatible settings
	h264enc, err := gst.NewElement("x264enc")
	if err != nil {
		return fmt.Errorf("failed to create h264enc: %v", err)
	}

	// Add h264 parser to ensure proper stream formatting
	h264parse, err := gst.NewElement("h264parse")
	if err != nil {
		return fmt.Errorf("failed to create h264parse: %v", err)
	}

	audioconv, err := gst.NewElement("audioconvert")
	if err != nil {
		return fmt.Errorf("failed to create audioconv: %v", err)
	}

	audioresample, err := gst.NewElement("audioresample")
	if err != nil {
		return fmt.Errorf("failed to create audioresample: %v", err)
	}

	// Add audio caps filter
	audiocaps, err := gst.NewElement("capsfilter")
	if err != nil {
		return fmt.Errorf("failed to create audiocaps: %v", err)
	}
	audiocapsstr := "audio/x-raw,rate=44100,channels=2"
	acaps := gst.NewCapsFromString(audiocapsstr)
	audiocaps.SetProperty("caps", acaps)

	aacenc, err := gst.NewElement("avenc_aac")
	if err != nil {
		return fmt.Errorf("failed to create aacenc: %v", err)
	}

	// MPEG-TS muxer
	mpegtsmux, err := gst.NewElement("mpegtsmux")
	if err != nil {
		return fmt.Errorf("failed to create mpegtsmux: %v", err)
	}
	mpegtsmux.SetProperty("name", "mux")

	// Direct UDP sink instead of RTP
	udpsink, err := gst.NewElement("udpsink")
	if err != nil {
		return fmt.Errorf("failed to create udpsink: %v", err)
	}

	// Set properties
	s.vselector.SetProperty("sync-streams", true)
	s.vselector.SetProperty("sync-mode", 1) // 1 = sync-to-clock
	s.aselector.SetProperty("sync-streams", true)
	s.aselector.SetProperty("sync-mode", 1)

	// Configure H264 encoder for better compatibility and performance
	h264enc.SetProperty("tune", "zerolatency")
	h264enc.SetProperty("bitrate", 2000)             // Set bitrate to 2Mbps for better quality
	h264enc.SetProperty("key-int-max", 30)           // Key frame every 30 frames
	h264enc.SetProperty("byte-stream", true)         // Use byte stream format for NAL units
	h264enc.SetProperty("speed-preset", "superfast") // Faster encoding
	h264enc.SetProperty("threads", 4)                // Use 4 threads for encoding

	aacenc.SetProperty("bitrate", 128000) // 128kbps audio

	// Set UDP properties for multicast
	udpsink.SetProperty("host", s.host)
	udpsink.SetProperty("port", s.port)
	udpsink.SetProperty("auto-multicast", true)
	udpsink.SetProperty("sync", true)             // Enable sync with clock
	udpsink.SetProperty("max-lateness", 10000000) // 10ms max lateness
	udpsink.SetProperty("buffer-size", 2097152)   // 2MB buffer size

	// Add elements to pipeline
	s.pipeline.Add(s.vselector)
	s.pipeline.Add(s.aselector)
	s.pipeline.Add(videoconv)
	s.pipeline.Add(videoscale)
	s.pipeline.Add(videocaps)
	s.pipeline.Add(h264enc)
	s.pipeline.Add(h264parse)
	s.pipeline.Add(audioconv)
	s.pipeline.Add(audioresample)
	s.pipeline.Add(audiocaps)
	s.pipeline.Add(aacenc)
	s.pipeline.Add(mpegtsmux)
	s.pipeline.Add(udpsink)

	// Link static elements for video path
	s.vselector.Link(videoconv)
	videoconv.Link(videoscale)
	videoscale.Link(videocaps)
	videocaps.Link(h264enc)
	h264enc.Link(h264parse)
	h264parse.Link(mpegtsmux)

	// Link static elements for audio path
	s.aselector.Link(audioconv)
	audioconv.Link(audioresample)
	audioresample.Link(audiocaps)
	audiocaps.Link(aacenc)
	aacenc.Link(mpegtsmux)

	// Link final elements - direct mpegts to udp
	mpegtsmux.Link(udpsink)

	// Add pad probes to drop late buffers
	vsrcpad := s.vselector.GetStaticPad("src")
	if vsrcpad != nil {
		vsrcpad.AddProbe(gst.PadProbeTypeBuffer, func(pad *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
			return gst.PadProbeOK
		})
	}

	asrcpad := s.aselector.GetStaticPad("src")
	if asrcpad != nil {
		asrcpad.AddProbe(gst.PadProbeTypeBuffer, func(pad *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
			return gst.PadProbeOK
		})
	}

	// Add pad probes to monitor caps negotiation
	vsrcpad = s.vselector.GetStaticPad("src")
	if vsrcpad != nil {
		vsrcpad.AddProbe(gst.PadProbeTypeEventDownstream, func(pad *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
			event := info.GetEvent()
			if event != nil && event.Type() == gst.EventTypeCaps {
				caps := event.ParseCaps()
				if caps != nil {
					fmt.Printf("Video caps from selector: %s\n", caps.String())
				}
			}
			return gst.PadProbeOK
		})
	}

	// Create a main loop
	s.mainLoop = glib.NewMainLoop(nil, false)

	// Create sources for each scheduled item
	s.mutex.Lock()
	items := make([]StreamItem, len(s.items))
	copy(items, s.items)
	s.mutex.Unlock()

	// Add sources to pipeline
	for i, item := range items {
		if item.Type == "file" {
			err := s.addFileSource(i, item.Source)
			if err != nil {
				return fmt.Errorf("failed to add source %d: %v", i, err)
			}
		}
		// Could add other source types here (test pattern, etc.)
	}

	// Set up the schedule
	go s.runSchedule()

	// Start the pipeline
	s.pipeline.SetState(gst.StatePlaying)
	fmt.Printf("Pipeline is running. Streaming to RTP at %s:%d\n", s.host, s.port)

	// Run the main loop in a separate goroutine
	go s.mainLoop.Run()

	return nil
}

// addFileSource adds a file source to the pipeline
func (s *StreamScheduler) addFileSource(index int, filePath string) error {
	// Create video source
	source, err := gst.NewElement("uridecodebin")
	if err != nil {
		return fmt.Errorf("failed to create source: %v", err)
	}
	source.SetProperty("name", fmt.Sprintf("source%d", index))

	// Make sure to handle relative paths
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %v", err)
	}

	source.SetProperty("uri", "file://"+absPath)

	// Add to pipeline
	s.pipeline.Add(source)

	// Connect pad-added signal
	source.Connect("pad-added", func(self *gst.Element, pad *gst.Pad) {
		caps := pad.CurrentCaps()
		if caps == nil {
			return
		}

		structure := caps.GetStructureAt(0)
		if structure == nil {
			return
		}

		name := structure.Name()

		if len(name) >= 5 && name[:5] == "video" {
			// Handle video pad
			// Check if pad already exists to avoid duplicate pad warnings
			padName := fmt.Sprintf("sink_%d", index)
			sinkPad := s.vselector.GetStaticPad(padName)
			if sinkPad == nil {
				sinkPad = s.vselector.GetRequestPad(padName)
				if sinkPad == nil {
					fmt.Printf("Failed to get request video pad %s\n", padName)
					return
				}
				pad.Link(sinkPad)
				fmt.Printf("Linked video source%d to vselector.%s\n", index, padName)
			} else {
				// Pad already exists, use it without requesting a new one
				pad.Link(sinkPad)
				fmt.Printf("Linked video source%d to existing vselector.%s\n", index, padName)
			}
		} else if len(name) >= 5 && name[:5] == "audio" {
			// Handle audio pad
			// Check if pad already exists to avoid duplicate pad warnings
			padName := fmt.Sprintf("sink_%d", index)
			sinkPad := s.aselector.GetStaticPad(padName)
			if sinkPad == nil {
				sinkPad = s.aselector.GetRequestPad(padName)
				if sinkPad == nil {
					fmt.Printf("Failed to get request audio pad %s\n", padName)
					return
				}
				pad.Link(sinkPad)
				fmt.Printf("Linked audio source%d to aselector.%s\n", index, padName)
			} else {
				// Pad already exists, use it without requesting a new one
				pad.Link(sinkPad)
				fmt.Printf("Linked audio source%d to existing aselector.%s\n", index, padName)
			}
		}
	})

	// Store source for later reference
	s.sources[index] = append(s.sources[index], source)

	return nil
}

// runSchedule manages the timing of the scheduled items
func (s *StreamScheduler) runSchedule() {
	s.mutex.Lock()
	items := make([]StreamItem, len(s.items))
	copy(items, s.items)
	s.mutex.Unlock()

	// Sort items by start time if needed

	for i, item := range items {
		// Calculate how long to wait until this item should start
		waitTime := time.Until(item.Start)
		if waitTime > 0 {
			select {
			case <-time.After(waitTime):
				// Time to switch to this source
				s.switchToSource(i)
			case <-s.stopChan:
				// Scheduler is stopping
				return
			}
		} else {
			// Start time is in the past, switch immediately
			s.switchToSource(i)
		}

		// Wait for the duration of this item
		select {
		case <-time.After(item.Duration):
			// Item duration complete
			continue
		case <-s.stopChan:
			// Scheduler is stopping
			return
		}
	}
}

// switchToSource switches the pipeline to use the specified source
func (s *StreamScheduler) switchToSource(index int) {
	fmt.Printf("Switching to source %d\n", index)

	// This needs to be executed in the main loop's context
	glib.IdleAdd(func() bool {
		sinkVideo := s.vselector.GetStaticPad(fmt.Sprintf("sink_%d", index))
		if sinkVideo != nil {
			s.vselector.SetProperty("active-pad", sinkVideo)
		}

		sinkAudio := s.aselector.GetStaticPad(fmt.Sprintf("sink_%d", index))
		if sinkAudio != nil {
			s.aselector.SetProperty("active-pad", sinkAudio)
		}

		return false
	})
}

// Stop stops the scheduler and pipeline
func (s *StreamScheduler) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return
	}

	// Signal scheduler to stop
	close(s.stopChan)
	s.running = false

	// Stop the pipeline
	if s.pipeline != nil {
		s.pipeline.SetState(gst.StateNull)
	}

	// Quit the main loop
	if s.mainLoop != nil {
		s.mainLoop.Quit()
	}
}
