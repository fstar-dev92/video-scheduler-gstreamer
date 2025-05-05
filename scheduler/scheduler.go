package scheduler

import (
	"fmt"
	"sync"
	"time"

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
	mutex     sync.Mutex
	stopChan  chan struct{}
	running   bool
	vselector *gst.Element
	aselector *gst.Element
}

// NewStreamScheduler creates a new stream scheduler
func NewStreamScheduler(host string, port int) (*StreamScheduler, error) {
	// Initialize GStreamer
	gst.Init(nil)

	// Create video and audio selectors
	vselector, err := gst.NewElement("input-selector")
	if err != nil {
		return nil, fmt.Errorf("failed to create video selector: %v", err)
	}

	aselector, err := gst.NewElement("input-selector")
	if err != nil {
		return nil, fmt.Errorf("failed to create audio selector: %v", err)
	}

	return &StreamScheduler{
		host:      host,
		port:      port,
		items:     make([]StreamItem, 0),
		stopChan:  make(chan struct{}),
		vselector: vselector,
		aselector: aselector,
	}, nil
}

// AddItem adds a scheduled item to play
func (s *StreamScheduler) AddItem(item StreamItem) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.items = append(s.items, item)
}

// RunSchedule manages the playback schedule
func (s *StreamScheduler) RunSchedule() error {
	s.mutex.Lock()
	if s.running {
		s.mutex.Unlock()
		return fmt.Errorf("scheduler is already running")
	}
	s.running = true

	// // Create a new GLib main loop
	// s.mainLoop = glib.NewMainLoop(nil, false)

	// // Start the main loop in a separate goroutine
	// go s.mainLoop.Run()

	items := make([]StreamItem, len(s.items))
	copy(items, s.items)
	s.mutex.Unlock()

	if len(items) == 0 {
		return fmt.Errorf("no items to schedule")
	}

	// Variables to track next pipeline
	var nextPipeline *gst.Pipeline
	var nextPipelineReady bool
	var nextPipelineMutex sync.Mutex

	// Function to prepare the next pipeline
	prepareNextPipeline := func(item StreamItem, index int) (*gst.Pipeline, error) {
		fmt.Printf("[%s] Preparing pipeline for item %d\n",
			time.Now().Format("15:04:05.000"), index)

		// Create a new pipeline
		pipeline, err := gst.NewPipeline(fmt.Sprintf("streaming-pipeline-%d", index))
		if err != nil {
			return nil, fmt.Errorf("failed to create pipeline: %v", err)
		}

		// Create source elements
		filesrc, err := gst.NewElement("filesrc")
		if err != nil {
			return nil, fmt.Errorf("failed to create filesrc: %v", err)
		}
		filesrc.SetProperty("location", item.Source)
		filesrc.SetProperty("blocksize", 1048576) // Increase to 1MB for better buffering
		filesrc.SetProperty("num-buffers", -1)    // No limit on buffers

		// Add a queue before decodebin for better buffering
		sourceQueue, err := gst.NewElement("queue")
		if err != nil {
			return nil, fmt.Errorf("failed to create sourceQueue: %v", err)
		}
		sourceQueue.SetProperty("max-size-buffers", 0)
		sourceQueue.SetProperty("max-size-bytes", 0)
		sourceQueue.SetProperty("max-size-time", uint64(10*time.Second)) // 10 second buffer

		// Add to pipeline
		pipeline.Add(sourceQueue)

		decodebin, err := gst.NewElement("decodebin")
		if err != nil {
			return nil, fmt.Errorf("failed to create decodebin: %v", err)
		}

		// Create identity elements for synchronization
		videoIdentity, err := gst.NewElement("identity")
		if err != nil {
			return nil, fmt.Errorf("failed to create video identity: %v", err)
		}

		audioIdentity, err := gst.NewElement("identity")
		if err != nil {
			return nil, fmt.Errorf("failed to create audio identity: %v", err)
		}

		// Create video conversion elements
		videoconv, err := gst.NewElement("videoconvert")
		if err != nil {
			return nil, fmt.Errorf("failed to create videoconv: %v", err)
		}

		videoscale, err := gst.NewElement("videoscale")
		if err != nil {
			return nil, fmt.Errorf("failed to create videoscale: %v", err)
		}

		// Add capsfilter to limit video size and framerate
		videocaps, err := gst.NewElement("capsfilter")
		if err != nil {
			return nil, fmt.Errorf("failed to create videocaps: %v", err)
		}

		// Set video to 720p max resolution at 30fps
		capstr := "video/x-raw,width=(int)[1,1920],height=(int)[1,1080],framerate=(fraction)[1/1,30/1]"
		caps := gst.NewCapsFromString(capstr)
		videocaps.SetProperty("caps", caps)

		// Create H.264 encoder and parser
		h264enc, err := gst.NewElement("x264enc")
		if err != nil {
			return nil, fmt.Errorf("failed to create h264enc: %v", err)
		}
		h264enc.SetProperty("tune", "zerolatency")
		h264enc.SetProperty("bitrate", 2000)             // 2Mbps
		h264enc.SetProperty("key-int-max", 30)           // Key frame every 30 frames
		h264enc.SetProperty("byte-stream", true)         // Use byte stream format
		h264enc.SetProperty("speed-preset", "superfast") // Faster encoding
		h264enc.SetProperty("threads", 4)                // Use 4 threads

		h264parse, err := gst.NewElement("h264parse")
		if err != nil {
			return nil, fmt.Errorf("failed to create h264parse: %v", err)
		}

		// Create audio conversion elements
		audioconv, err := gst.NewElement("audioconvert")
		if err != nil {
			return nil, fmt.Errorf("failed to create audioconv: %v", err)
		}

		audioresample, err := gst.NewElement("audioresample")
		if err != nil {
			return nil, fmt.Errorf("failed to create audioresample: %v", err)
		}

		// Add audio caps filter
		audiocaps, err := gst.NewElement("capsfilter")
		if err != nil {
			return nil, fmt.Errorf("failed to create audiocaps: %v", err)
		}
		audiocapsstr := "audio/x-raw,rate=44100,channels=2"
		acaps := gst.NewCapsFromString(audiocapsstr)
		audiocaps.SetProperty("caps", acaps)

		// Create AAC encoder
		aacenc, err := gst.NewElement("avenc_aac")
		if err != nil {
			return nil, fmt.Errorf("failed to create aacenc: %v", err)
		}
		aacenc.SetProperty("bitrate", 128000) // 128kbps audio

		// Create MPEG-TS muxer
		mpegtsmux, err := gst.NewElement("mpegtsmux")
		if err != nil {
			return nil, fmt.Errorf("failed to create mpegtsmux: %v", err)
		}
		mpegtsmux.SetProperty("alignment", 7)                     // GST_MPEG_TS_MUX_ALIGNMENT_ALIGNED
		mpegtsmux.SetProperty("pat-interval", int64(100*1000000)) // 100ms
		mpegtsmux.SetProperty("pmt-interval", int64(100*1000000)) // 100ms
		mpegtsmux.SetProperty("pcr-interval", int64(20*1000000))  // 20ms

		// Create final identity element
		finalIdentity, err := gst.NewElement("identity")
		if err != nil {
			return nil, fmt.Errorf("failed to create final identity: %v", err)
		}
		finalIdentity.SetProperty("single-segment", true)
		finalIdentity.SetProperty("sync", true)

		// Create RTP payloader for MPEG-TS
		rtpmp2tpay, err := gst.NewElement("rtpmp2tpay")
		if err != nil {
			return nil, fmt.Errorf("failed to create rtpmp2tpay: %v", err)
		}
		rtpmp2tpay.SetProperty("pt", 33) // Payload type for MPEG-TS

		// Create UDP sink
		udpsink, err := gst.NewElement("udpsink")
		if err != nil {
			return nil, fmt.Errorf("failed to create udpsink: %v", err)
		}
		udpsink.SetProperty("host", s.host)
		udpsink.SetProperty("port", s.port)
		udpsink.SetProperty("auto-multicast", true)
		// udpsink.SetProperty("buffer-size", 2097152)   // 2MB buffer size
		udpsink.SetProperty("max-lateness", -1) // Don't drop late buffers
		udpsink.SetProperty("ts-offset", 0)     // No timestamp offset

		udpsink.SetProperty("sync", false)         // Don't sync to clock
		udpsink.SetProperty("async", false)        // Don't use async buffering
		udpsink.SetProperty("buffer-size", 262144) // Smaller buffer (256KB)

		// Reduce latency in mpegtsmux
		mpegtsmux.SetProperty("latency", 0) // Minimize muxer latency

		// Set low-latency tuning for h264enc
		h264enc.SetProperty("tune", "zerolatency")
		h264enc.SetProperty("speed-preset", "ultrafast")
		h264enc.SetProperty("key-int-max", 15) // More frequent keyframes

		// Set pipeline to low latency mode
		pipeline.SetProperty("latency", 0)

		// Configure identity elements
		videoIdentity.SetProperty("sync", true)
		audioIdentity.SetProperty("sync", true)
		rtpQueue, err := gst.NewElement("queue")
		if err != nil {
			return nil, fmt.Errorf("failed to create rtpQueue: %v", err)
		}
		rtpQueue.SetProperty("max-size-buffers", 200)
		rtpQueue.SetProperty("max-size-time", uint64(500*time.Microsecond))
		rtpQueue.SetProperty("leaky", 2)
		// Add all elements to pipeline
		pipeline.Add(filesrc)
		pipeline.Add(sourceQueue)
		pipeline.Add(decodebin)
		pipeline.Add(videoIdentity)
		pipeline.Add(audioIdentity)
		pipeline.Add(videoconv)
		pipeline.Add(videoscale)
		pipeline.Add(videocaps)
		pipeline.Add(h264enc)
		pipeline.Add(h264parse)
		pipeline.Add(audioconv)
		pipeline.Add(audioresample)
		pipeline.Add(audiocaps)
		pipeline.Add(aacenc)
		pipeline.Add(mpegtsmux)
		pipeline.Add(finalIdentity)
		pipeline.Add(rtpmp2tpay)
		pipeline.Add(udpsink)
		pipeline.Add(rtpQueue)

		// Link filesrc to sourceQueue
		filesrc.Link(sourceQueue)
		sourceQueue.Link(decodebin)
		rtpmp2tpay.Link(rtpQueue)
		rtpQueue.Link(udpsink)

		// Set up bus to watch for messages
		bus := pipeline.GetBus()

		// Connect to decodebin's pad-added signal
		decodebin.Connect("pad-added", func(element *gst.Element, pad *gst.Pad) {
			caps := pad.CurrentCaps()
			if caps == nil {
				fmt.Printf("Warning: pad %s has no caps\n", pad.GetName())
				return
			}

			structure := caps.GetStructureAt(0)
			name := structure.Name()

			fmt.Printf("Pad added with caps: %s\n", name)

			if len(name) >= 5 && name[:5] == "video" {
				// Link video path
				sinkpad := videoIdentity.GetStaticPad("sink")
				if sinkpad == nil {
					fmt.Println("Error: couldn't get sink pad from videoIdentity")
					return
				}

				if pad.Link(sinkpad) != gst.PadLinkOK {
					fmt.Println("Error: couldn't link video pad")
					return
				}

				fmt.Println("Linked video pad successfully")

				// Link the rest of the video path
				videoIdentity.Link(videoconv)
				videoconv.Link(videoscale)
				videoscale.Link(videocaps)
				videocaps.Link(h264enc)
				h264enc.Link(h264parse)
				h264parse.Link(mpegtsmux)

			} else if len(name) >= 5 && name[:5] == "audio" {
				// Link audio path
				sinkpad := audioIdentity.GetStaticPad("sink")
				if sinkpad == nil {
					fmt.Println("Error: couldn't get sink pad from audioIdentity")
					return
				}

				if pad.Link(sinkpad) != gst.PadLinkOK {
					fmt.Println("Error: couldn't link audio pad")
					return
				}

				fmt.Println("Linked audio pad successfully")

				// Link the rest of the audio path
				audioIdentity.Link(audioconv)
				audioconv.Link(audioresample)
				audioresample.Link(audiocaps)
				audiocaps.Link(aacenc)
				aacenc.Link(mpegtsmux)
			}
		})

		// Link muxer to final identity to RTP payloader to UDP sink
		mpegtsmux.Link(finalIdentity)
		finalIdentity.Link(rtpmp2tpay)
		rtpmp2tpay.Link(udpsink)

		// Set up a message handler to catch errors
		go func() {
			for {
				msg := bus.TimedPop(gst.ClockTimeNone)
				if msg == nil {
					break
				}

				switch msg.Type() {
				case gst.MessageError:
					gerr := msg.ParseError()
					fmt.Printf("Error from element %s: %s\n", msg.Source(), gerr.Error())

				case gst.MessageWarning:
					gerr := msg.ParseWarning()
					fmt.Printf("Warning from element %s: %s\n", msg.Source(), gerr.Error())
				case gst.MessageEOS:
					fmt.Printf("[%s] End of stream received from %s - ignoring to continue playback\n",
						time.Now().Format("15:04:05.000"), msg.Source())
					// Don't do anything with EOS, let the duration timer handle switching
				}
			}
		}()

		fmt.Println("Setting pipeline to PAUSED state for preroll...")

		// Set pipeline to PAUSED to preroll
		pipeline.SetState(gst.StatePaused)

		fmt.Printf("[%s] Pipeline for item %d prerolled successfully\n",
			time.Now().Format("15:04:05.000"), index)

		return pipeline, nil
	}

	// Play each item sequentially
	for i, item := range items {
		var currentPipeline *gst.Pipeline

		// If this isn't the first item, check if we have a prepared next pipeline
		if i > 0 {
			nextPipelineMutex.Lock()
			if nextPipelineReady {
				currentPipeline = nextPipeline
				nextPipeline = nil
				nextPipelineReady = false
				fmt.Printf("[%s] Using prepared pipeline for item %d\n",
					time.Now().Format("15:04:05.000"), i)
			}
			nextPipelineMutex.Unlock()
		}

		// If we don't have a prepared pipeline, create one now
		if currentPipeline == nil {
			fmt.Printf("[%s] Item %d: Creating pipeline on demand\n",
				time.Now().Format("15:04:05.000"), i)

			var err error
			currentPipeline, err = prepareNextPipeline(item, i)
			if err != nil {
				fmt.Printf("Failed to create pipeline for item %d: %v\n", i, err)
				continue
			}
		}

		// Start playing the current pipeline
		s.mutex.Lock()
		s.pipeline = currentPipeline
		s.mutex.Unlock()

		currentPipeline.SetState(gst.StatePlaying)
		fmt.Printf("[%s] Item %d: Started playing\n",
			time.Now().Format("15:04:05.000"), i)
		time.Sleep(100 * time.Millisecond)

		// If there's a next item, start preparing its pipeline immediately
		if i+1 < len(items) {
			go func(nextItem StreamItem, nextIndex int) {
				preparedPipeline, err := prepareNextPipeline(nextItem, nextIndex)
				if err != nil {
					fmt.Printf("Failed to prepare pipeline for item %d: %v\n", nextIndex, err)
					return
				}

				nextPipelineMutex.Lock()
				nextPipeline = preparedPipeline
				nextPipelineReady = true
				nextPipelineMutex.Unlock()
			}(items[i+1], i+1)
		}

		// Monitor pipeline position to ensure we stop at exact duration
		durationReached := make(chan struct{})
		go func() {
			startTime := time.Now()
			targetDuration := item.Duration

			// Use wall clock as backup if pipeline position doesn't work
			for time.Since(startTime) < targetDuration*2 { // Double duration as safety
				// Query pipeline position
				ok, position := currentPipeline.QueryPosition(gst.FormatTime)
				if ok && position > 0 {
					positionNs := position
					durationNs := int64(targetDuration.Nanoseconds())

					// Log position for debugging
					if positionNs%(1000000000) < 50000000 { // Log every second
						fmt.Printf("[%s] Item %d: Position: %.2fs / %.2fs (Elapsed: %.2fs)\n",
							time.Now().Format("15:04:05.000"), i,
							float64(positionNs)/1000000000.0,
							float64(durationNs)/1000000000.0,
							time.Since(startTime).Seconds())
					}

					// If we've reached or exceeded the specified duration, stop playback
					if positionNs >= durationNs {
						fmt.Printf("[%s] Item %d: Reached specified duration %.2fs\n",
							time.Now().Format("15:04:05.000"), i,
							float64(durationNs)/1000000000.0)
						close(durationReached)
						return
					}
				} else {
					// If we can't query position, use wall clock as fallback
					if time.Since(startTime) >= targetDuration {
						fmt.Printf("[%s] Item %d: Duration reached via wall clock\n",
							time.Now().Format("15:04:05.000"), i)
						close(durationReached)
						return
					}
				}
				time.Sleep(20 * time.Millisecond) // Check position frequently
			}

			// Fallback - if we get here, use wall clock
			fmt.Printf("[%s] Item %d: Fallback - using wall clock\n",
				time.Now().Format("15:04:05.000"), i)
			close(durationReached)
		}()
		// Wait for duration reached or stop signal
		select {
		case <-durationReached:
			fmt.Printf("[%s] Item %d: Duration reached via position monitoring\n",
				time.Now().Format("15:04:05.000"), i)

			// If there's a next pipeline ready, start it BEFORE stopping the current one
			if i+1 < len(items) && nextPipelineReady {
				nextPipelineMutex.Lock()
				if nextPipelineReady && nextPipeline != nil {
					fmt.Printf("[%s] Starting next pipeline before stopping current\n",
						time.Now().Format("15:04:05.000"))

					// Set the next pipeline as current
					s.mutex.Lock()
					nextPipeline.SetState(gst.StatePlaying)
					s.pipeline = nextPipeline
					s.mutex.Unlock()

					// Small overlap to ensure smooth transition
					time.Sleep(200 * time.Millisecond)
				}
				nextPipelineMutex.Unlock()
			}

			// Now stop the current pipeline
			currentPipeline.SetState(gst.StateNull)

		case <-s.stopChan:
			fmt.Printf("[%s] Item %d: Received stop signal during playback\n",
				time.Now().Format("15:04:05.000"), i)

			// Stop the current pipeline
			currentPipeline.SetState(gst.StateNull)

			// Also stop any pipeline being prepared
			nextPipelineMutex.Lock()
			if nextPipelineReady && nextPipeline != nil {
				nextPipeline.SetState(gst.StateNull)
			}
			nextPipelineMutex.Unlock()

			return nil
		}

		// Stop the current pipeline
		currentPipeline.SetState(gst.StateNull)
	}

	// All items have been played
	s.mutex.Lock()
	s.running = false
	s.mutex.Unlock()

	return nil
}

// Stop stops the scheduler and all pipelines
func (s *StreamScheduler) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return
	}

	// Signal scheduler to stop
	close(s.stopChan)
	s.running = false

	// Stop the current pipeline
	if s.pipeline != nil {
		s.pipeline.SetState(gst.StateNull)
		s.pipeline = nil
	}
}
