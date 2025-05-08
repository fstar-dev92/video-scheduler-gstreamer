// ... existing code ...
package scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
	"github.com/google/uuid"
)

type StreamItem struct {
	Type      string
	Source    string
	Start     time.Time
	Duration  time.Duration
	Offset    time.Duration
	NeedBreak bool
}

type Bin struct {
	Bin        *gst.Bin
	VideoQueue *gst.Element
	AudioQueue *gst.Element
	Item       StreamItem
}

type StreamScheduler struct {
	host            string
	port            int
	items           []StreamItem
	mainPipeline    *gst.Pipeline
	sourcePipelines []*gst.Pipeline
	compositor      *gst.Element
	mutex           sync.Mutex
	stopChan        chan struct{}
	switchNext      chan struct{}
	running         bool
	currentIndex    int
	schedulerID     string
}

func NewStreamScheduler(host string, port int) (*StreamScheduler, error) {
	gst.Init(nil)

	return &StreamScheduler{
		host:        host,
		port:        port,
		items:       make([]StreamItem, 0),
		stopChan:    make(chan struct{}),
		switchNext:  make(chan struct{}, 5), // Buffered channel to prevent blocking
		schedulerID: uuid.New().String(),
	}, nil
}

func (s *StreamScheduler) AddItem(item StreamItem) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.items = append(s.items, item)
}

func (s *StreamScheduler) RunSchedule() error {
	s.mutex.Lock()
	if s.running {
		s.mutex.Unlock()
		return fmt.Errorf("scheduler is already running")
	}
	s.running = true

	items := make([]StreamItem, len(s.items))
	copy(items, s.items)
	s.mutex.Unlock()

	if len(items) == 0 {
		return fmt.Errorf("no items to schedule")
	}

	// Create and start the main pipeline
	err := s.createMainPipeline()
	if err != nil {
		return fmt.Errorf("failed to create main pipeline: %v", err)
	}

	s.mainPipeline.SetState(gst.StatePlaying)

	// Start playing items
	s.currentIndex = 0
	s.playCurrentItem(items)

	// Main loop to handle item switching
	go func() {
		for s.running {
			select {
			case <-s.switchNext:
				s.currentIndex++
				if s.currentIndex >= len(items) {
					s.currentIndex = 0 // Loop back to the beginning
				}
				s.playCurrentItem(items)

			case <-time.After(items[s.currentIndex].Duration):
				// Make sure we don't block if channel is full
				select {
				case s.switchNext <- struct{}{}:
					// Successfully sent signal
				default:
					// Channel is full, log and continue
					fmt.Printf("[%s] Warning: switchNext channel is full, skipping signal\n",
						time.Now().Format("15:04:05.000"))
					s.currentIndex++
					if s.currentIndex >= len(items) {
						s.currentIndex = 0
					}
					s.playCurrentItem(items)
				}

			case <-s.stopChan:
				s.cleanupPipelines()
				return
			}
		}
	}()

	return nil
}

func (s *StreamScheduler) createMainPipeline() error {
	pipeline, err := gst.NewPipeline("main-pipeline" + s.schedulerID)
	if err != nil {
		return fmt.Errorf("failed to create main pipeline: %v", err)
	}

	// Create two intervideosrc elements for the two input channels
	intervideo1, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input1" + s.schedulerID,
		"do-timestamp": true,
		"name":         "intervideosrc1" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosrc1: %v", err)
	}

	intervideo2, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input2" + s.schedulerID,
		"do-timestamp": true,
		"name":         "intervideosrc2" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosrc2: %v", err)
	}

	// Create video mixer (compositor)
	s.compositor, err = gst.NewElementWithProperties("compositor", map[string]interface{}{
		"background":            1, // black background
		"zero-size-is-unscaled": true,
		"name":                  "compositor" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create compositor: %v", err)
	}

	// Configure compositor sink pads for proper positioning
	pad1 := s.compositor.GetStaticPad("sink_0")
	if pad1 != nil {
		pad1.SetProperty("xpos", 0)
		pad1.SetProperty("ypos", 0)
		pad1.SetProperty("width", 1920)
		pad1.SetProperty("height", 1080)
		pad1.SetProperty("alpha", 1.0) // input1 visible
	}

	pad2 := s.compositor.GetStaticPad("sink_1")
	if pad2 != nil {
		pad2.SetProperty("xpos", 0)
		pad2.SetProperty("ypos", 0)
		pad2.SetProperty("width", 1920)
		pad2.SetProperty("height", 1080)
		pad2.SetProperty("alpha", 0.0) // input2 hidden
	}

	// Create video converter and encoder
	videoconv, err := gst.NewElement("videoconvert")
	if err != nil {
		return fmt.Errorf("failed to create videoconvert: %v", err)
	}

	h264enc, err := gst.NewElementWithProperties("x264enc", map[string]interface{}{
		"tune":    0x00000004, // zerolatency
		"bitrate": 2000,       // 2 Mbps
		"name":    "h264enc" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create h264enc: %v", err)
	}

	// Create audio elements (interaudiosrc, audiomixer, etc.)
	interaudio1, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio1" + s.schedulerID,
		"do-timestamp": true,
		"name":         "interaudiosrc1" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosrc1: %v", err)
	}

	interaudio2, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio2" + s.schedulerID,
		"do-timestamp": true,
		"name":         "interaudiosrc2" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosrc2: %v", err)
	}

	audiomixer, err := gst.NewElement("audiomixer")
	audiomixer.SetProperty("name", "audiomixer" + s.schedulerID)
	if err != nil {
		return fmt.Errorf("failed to create audiomixer: %v", err)
	}

	audioconv, err := gst.NewElement("audioconvert")
	audioconv.SetProperty("name", "audioconv" + s.schedulerID)
	if err != nil {
		return fmt.Errorf("failed to create audioconvert: %v", err)
	}

	aacenc, err := gst.NewElementWithProperties("avenc_aac", map[string]interface{}{
		"bitrate": 128000,
		"name":    "aacenc" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create aacenc: %v", err)
	}

	// Create muxer and RTP elements
	mpegtsmux, err := gst.NewElementWithProperties("mpegtsmux", map[string]interface{}{
		"alignment":    7,
		"pat-interval": int64(100 * 1000000),
		"pmt-interval": int64(100 * 1000000),
		"pcr-interval": int64(20 * 1000000),
		"name":         "mpegtsmux" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create mpegtsmux: %v", err)
	}

	rtpmp2tpay, err := gst.NewElementWithProperties("rtpmp2tpay", map[string]interface{}{
		"pt":              33,
		"mtu":             1400,
		"perfect-rtptime": true,
		"name":           "rtpmp2tpay" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}

	udpsink, err := gst.NewElementWithProperties("udpsink", map[string]interface{}{
		"host":           s.host,
		"port":           s.port,
		"sync":           true,
		"buffer-size":    524288,
		"auto-multicast": true,
		"name":           "udpsink" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create udpsink: %v", err)
	}

	// Add queues to manage latency in the main pipeline
	videoQueue1, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0, // No leaking
		"name":               "videoQueue1" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create videoQueue1: %v", err)
	}

	videoQueue2, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0, // No leaking
		"name":               "videoQueue2" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create videoQueue2: %v", err)
	}

	audioQueue1, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0, // No leaking
		"name":               "audioQueue1" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audioQueue1: %v", err)
	}

	audioQueue2, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0, // No leaking
		"name":               "audioQueue2" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audioQueue2: %v", err)
	}

	// Add queues after mixer/compositor
	videoMixerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0, // No leaking
		"name":               "videoMixerQueue" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create videoMixerQueue: %v", err)
	}

	audioMixerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   100,
		"max-size-time":      uint64(500 * time.Millisecond),
		"min-threshold-time": uint64(50 * time.Millisecond),
		"leaky":              0, // No leaking
		"name":               "audioMixerQueue" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audioMixerQueue: %v", err)
	}

	// Add final queue before muxer
	muxerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"max-size-buffers":   200,
		"max-size-time":      uint64(1 * time.Second),
		"min-threshold-time": uint64(100 * time.Millisecond),
		"leaky":              0, // No leaking
		"name":               "muxerQueue" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create muxerQueue: %v", err)
	}

	// Create audio converter elements for each input
	audioconv1, err := gst.NewElement("audioconvert")
	audioconv1.SetProperty("name", "audioconv1" + s.schedulerID)
	if err != nil {
		return fmt.Errorf("failed to create audioconv1: %v", err)
	}

	audioconv2, err := gst.NewElement("audioconvert")
	audioconv2.SetProperty("name", "audioconv2" + s.schedulerID)
	if err != nil {
		return fmt.Errorf("failed to create audioconv2: %v", err)
	}

	// Create audioresample elements to ensure rate compatibility
	audioresample1, err := gst.NewElement("audioresample")
	audioresample1.SetProperty("name", "audioresample1" + s.schedulerID)
	if err != nil {
		return fmt.Errorf("failed to create audioresample1: %v", err)
	}

	audioresample2, err := gst.NewElement("audioresample")
	audioresample2.SetProperty("name", "audioresample2" + s.schedulerID)
	if err != nil {
		return fmt.Errorf("failed to create audioresample2: %v", err)
	}

	// Create capsfilters to explicitly set audio format
	audiocaps1, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": gst.NewCapsFromString("audio/x-raw, format=S16LE, layout=interleaved, rate=48000, channels=2"),
		"name": "audiocaps1" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audiocaps1: %v", err)
	}

	audiocaps2, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": gst.NewCapsFromString("audio/x-raw, format=S16LE, layout=interleaved, rate=48000, channels=2"),
		"name": "audiocaps2" + s.schedulerID,
	})
	if err != nil {
		return fmt.Errorf("failed to create audiocaps2: %v", err)
	}

	// Add all new elements to the pipeline
	pipeline.AddMany(audioconv1, audioconv2, audioresample1, audioresample2, audiocaps1, audiocaps2)

	// Add all elements to the pipeline
	pipeline.AddMany(intervideo1, intervideo2, s.compositor, videoconv, h264enc)
	pipeline.AddMany(interaudio1, interaudio2, audiomixer, audioconv, aacenc)
	pipeline.AddMany(mpegtsmux, rtpmp2tpay, udpsink)
	pipeline.AddMany(videoQueue1, videoQueue2, audioQueue1, audioQueue2)
	pipeline.AddMany(videoMixerQueue, audioMixerQueue, muxerQueue)

	// Link video elements
	intervideo1.Link(videoQueue1)
	videoQueue1.Link(s.compositor)
	intervideo2.Link(videoQueue2)
	videoQueue2.Link(s.compositor)
	s.compositor.Link(videoMixerQueue)
	videoMixerQueue.Link(videoconv)
	videoconv.Link(h264enc)
	h264enc.Link(muxerQueue)
	muxerQueue.Link(mpegtsmux)

	// Link audio elements
	interaudio1.Link(audioQueue1)
	audioQueue1.Link(audioconv1)
	audioconv1.Link(audioresample1)
	audioresample1.Link(audiocaps1)
	audiocaps1.Link(audiomixer)

	interaudio2.Link(audioQueue2)
	audioQueue2.Link(audioconv2)
	audioconv2.Link(audioresample2)
	audioresample2.Link(audiocaps2)
	audiocaps2.Link(audiomixer)

	audiomixer.Link(audioMixerQueue)
	audioMixerQueue.Link(audioconv)
	audioconv.Link(aacenc)
	aacenc.Link(mpegtsmux)

	// Link muxer to RTP and UDP sink
	mpegtsmux.Link(rtpmp2tpay)
	rtpmp2tpay.Link(udpsink)

	// Set up bus watch
	bus := pipeline.GetBus()
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
				fmt.Printf("End of stream received\n")
			}
		}
	}()

	s.mainPipeline = pipeline
	return nil
}

func (s *StreamScheduler) createSourcePipeline(item StreamItem, index int, channel string) (*gst.Pipeline, error) {
	pipelineName := fmt.Sprintf("source-pipeline-%d", index)
	pipeline, err := gst.NewPipeline(pipelineName)
	if err != nil {
		return nil, fmt.Errorf("failed to create source pipeline: %v", err)
	}

	// Create playbin for easy media handling
	playbin, err := gst.NewElementWithProperties("playbin3", map[string]interface{}{
		"uri": fmt.Sprintf("file://%s", item.Source),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create playbin: %v", err)
	}

	// Add buffering properties to playbin
	playbin.SetProperty("buffer-size", 10485760) // 10MB buffer
	playbin.SetProperty("buffer-duration", uint64(5*time.Second))
	playbin.SetProperty("low-percent", 10)
	playbin.SetProperty("high-percent", 99)
	playbin.SetProperty("use-buffering", true)

	// Create video sink
	intervideosink, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel": channel + s.schedulerID,
		"sync":    true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosink: %v", err)
	}

	// Add latency control to the video sink
	intervideosink.SetProperty("max-lateness", int64(20*time.Millisecond))
	intervideosink.SetProperty("qos", true)

	// Set video sink on playbin
	playbin.SetProperty("video-sink", intervideosink)

	// Create audio bin with format conversion
	audiobin := gst.NewBin("audiobin")

	// Create elements for audio conversion
	audioconvert, err := gst.NewElement("audioconvert")
	if err != nil {
		return nil, fmt.Errorf("failed to create audioconvert: %v", err)
	}

	audioresample, err := gst.NewElement("audioresample")
	if err != nil {
		return nil, fmt.Errorf("failed to create audioresample: %v", err)
	}

	audiocaps, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": gst.NewCapsFromString("audio/x-raw, format=S16LE, layout=interleaved, rate=48000, channels=2"),
		"name": "audiocaps" + s.schedulerID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audiocaps: %v", err)
	}

	// Create audio sink with proper format
	interaudiosink, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel": fmt.Sprintf("audio%s", channel[len(channel)-1:]) + s.schedulerID,
		"sync":    true,
		"name":    "interaudiosink" + s.schedulerID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosink: %v", err)
	}

	// Add elements to bin
	audiobin.AddMany(audioconvert, audioresample, audiocaps, interaudiosink)

	// Link elements in bin
	audioconvert.Link(audioresample)
	audioresample.Link(audiocaps)
	audiocaps.Link(interaudiosink)

	// Create and add ghost pad using the bin's method
	sinkpad := audioconvert.GetStaticPad("sink")
	if sinkpad == nil {
		return nil, fmt.Errorf("failed to get sink pad from audioconvert")
	}

	ghostpad := gst.NewGhostPad("sink", sinkpad)
	if ghostpad == nil {
		return nil, fmt.Errorf("failed to create ghost pad")
	}

	if !audiobin.AddPad(ghostpad.Pad) {
		return nil, fmt.Errorf("failed to add ghost pad to bin")
	}

	// Set audio-sink property on playbin to use our bin
	playbin.SetProperty("audio-sink", audiobin)

	// Add playbin to pipeline
	pipeline.Add(playbin)

	// Set up bus watch
	bus := pipeline.GetBus()
	go func() {
		for {
			msg := bus.TimedPop(gst.ClockTimeNone)
			if msg == nil {
				break
			}

			switch msg.Type() {
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("[%s] Error from source %d: %s\n",
					time.Now().Format("15:04:05.000"), index, gerr.Error())
				// Try to recover by scheduling the next item
				s.switchNext <- struct{}{}
			case gst.MessageEOS:
				fmt.Printf("[%s] End of stream for source %d\n",
					time.Now().Format("15:04:05.000"), index)
				s.switchNext <- struct{}{}
			case gst.MessageStateChanged:
				if msg.Source() == pipeline.Element.GetName() {
					oldState, newState := msg.ParseStateChanged()
					if oldState == gst.StatePaused && newState == gst.StatePlaying {
						fmt.Printf("[%s] Pipeline %d state changed to PLAYING\n",
							time.Now().Format("15:04:05.000"), index)
					}
				}
			}
		}
	}()

	return pipeline, nil
}

func (s *StreamScheduler) playCurrentItem(items []StreamItem) {
	fmt.Printf("[%s] Preparing to play item %d: %s\n",
		time.Now().Format("15:04:05.000"), s.currentIndex, items[s.currentIndex].Source)

	// Clean up any existing source pipelines
	for _, pipeline := range s.sourcePipelines {
		pipeline.SetState(gst.StateNull)
	}
	s.sourcePipelines = nil

	// Determine which channel to use (alternating between 1 and 2)
	channel := fmt.Sprintf("input%d", (s.currentIndex%2)+1)

	// Create new source pipeline
	pipeline, err := s.createSourcePipeline(items[s.currentIndex], s.currentIndex, channel)
	if err != nil {
		fmt.Printf("[%s] Error creating source pipeline: %v\n",
			time.Now().Format("15:04:05.000"), err)
		// Try to recover by scheduling the next item
		time.AfterFunc(1*time.Second, func() {
			s.switchNext <- struct{}{}
		})
		return
	}

	s.sourcePipelines = append(s.sourcePipelines, pipeline)

	// Start playing
	err1 := pipeline.SetState(gst.StatePlaying)
	if err1 != nil {
		fmt.Printf("[%s] Failed to set pipeline to playing state:\n",
			time.Now().Format("15:04:05.000"))
		// Try to recover by scheduling the next item
		time.AfterFunc(1*time.Second, func() {
			s.switchNext <- struct{}{}
		})
		return
	}

	fmt.Printf("[%s] Started playing item %d: %s on channel %s\n",
		time.Now().Format("15:04:05.000"), s.currentIndex, items[s.currentIndex].Source, channel)

	// Handle seeking to offset if needed
	if items[s.currentIndex].Offset > 0 {
		go func(offset time.Duration) {
			// Wait a moment for pipeline to stabilize
			time.Sleep(200 * time.Millisecond)
			fmt.Printf("[%s] Seeking to offset %v\n",
				time.Now().Format("15:04:05.000"), offset)

			// Perform seek operation
			ret := pipeline.SeekTime(
				offset,
				gst.SeekFlagFlush|gst.SeekFlagKeyUnit,
			)
			if !ret {
				fmt.Printf("[%s] Failed to seek to offset\n",
					time.Now().Format("15:04:05.000"))
			}
		}(items[s.currentIndex].Offset)
	}

	// Toggle visibility based on current channel
	if channel == "input1" {

		pad1 := s.compositor.GetStaticPad("sink_0")
		pad2 := s.compositor.GetStaticPad("sink_1")
		if pad1 != nil && pad2 != nil {
			pad1.SetProperty("alpha", 1.0) // Show input1
			pad2.SetProperty("alpha", 0.0) // Hide input2
		}
	} else {

		pad1 := s.compositor.GetStaticPad("sink_0")
		pad2 := s.compositor.GetStaticPad("sink_1")
		if pad1 != nil && pad2 != nil {
			pad1.SetProperty("alpha", 0.0) // Hide input1
			pad2.SetProperty("alpha", 1.0) // Show input2
		}
	}
}

func (s *StreamScheduler) cleanupPipelines() {
	// Stop all pipelines
	if s.mainPipeline != nil {
		s.mainPipeline.SetState(gst.StateNull)
	}

	for _, pipeline := range s.sourcePipelines {
		pipeline.SetState(gst.StateNull)
	}
}

func (s *StreamScheduler) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return
	}

	close(s.stopChan)
	s.running = false
	s.cleanupPipelines()
}
