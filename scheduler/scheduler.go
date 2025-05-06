package scheduler

import (
	"fmt"
	"net"
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
	host             string
	port             int
	items            []StreamItem
	pipeline         *gst.Pipeline
	mutex            sync.Mutex
	stopChan         chan struct{}
	running          bool
	vselector        *gst.Element
	aselector        *gst.Element
	lastEndTimestamp int64 // Track the last end timestamp for continuity
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

		// Create a new pipeline with a unique name
		pipeline, err := gst.NewPipeline(fmt.Sprintf("streaming-pipeline-%d", index))
		if err != nil {
			return nil, fmt.Errorf("failed to create pipeline: %v", err)
		}

		// Create decodebin with unique name
		// Create source elements
		filesrc, err := gst.NewElementWithProperties("filesrc", map[string]interface{}{
			"location":  item.Source,
			"blocksize": 1048576, // 1MB for better buffering
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create filesrc: %v", err)
		}

		// Create a queue before decodebin for better buffering
		sourceQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
			"max-size-buffers": 0,
			"max-size-bytes":   0,
			"max-size-time":    uint64(10 * time.Second), // 10 second buffer
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create sourceQueue: %v", err)
		}

		// Create decodebin
		decodebin, err := gst.NewElement("decodebin")
		if err != nil {
			return nil, fmt.Errorf("failed to create decodebin: %v", err)
		}

		// Add elements to pipeline
		pipeline.AddMany(filesrc, sourceQueue, decodebin)

		// Link elements
		filesrc.Link(sourceQueue)
		sourceQueue.Link(decodebin)

		// Create video processing elements
		videoIdentity, err := gst.NewElementWithProperties("identity", map[string]interface{}{
			"sync": true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create video identity: %v", err)
		}

		videoConvert, err := gst.NewElement("videoconvert")
		if err != nil {
			return nil, fmt.Errorf("failed to create videoconvert: %v", err)
		}

		videoScale, err := gst.NewElement("videoscale")
		if err != nil {
			return nil, fmt.Errorf("failed to create videoscale: %v", err)
		}

		// Add capsfilter to limit video size and framerate
		videoCaps := gst.NewCapsFromString("video/x-raw,width=(int)[1,1920],height=(int)[1,1080],framerate=(fraction)[1/1,60/1]")
		videoCapsFilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
			"caps": videoCaps,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create videocaps: %v", err)
		}

		// Create H.264 encoder with optimized settings
		h264enc, err := gst.NewElementWithProperties("x264enc", map[string]interface{}{
			"tune":             0,
			"bitrate":          2000, // 2Mbps
			"key-int-max":      60,   // Key frame every 60 frames
			"speed-preset":     1,    // Faster encoding
			"threads":          4,    // Use 4 threads
			"vbv-buf-capacity": 2000, // 2000ms VBV buffer
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create h264enc: %v", err)
		}

		h264parse, err := gst.NewElement("h264parse")
		if err != nil {
			return nil, fmt.Errorf("failed to create h264parse: %v", err)
		}

		// Create audio processing elements
		audioIdentity, err := gst.NewElementWithProperties("identity", map[string]interface{}{
			"sync": true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create audio identity: %v", err)
		}

		audioConvert, err := gst.NewElement("audioconvert")
		if err != nil {
			return nil, fmt.Errorf("failed to create audioconvert: %v", err)
		}

		audioResample, err := gst.NewElement("audioresample")
		if err != nil {
			return nil, fmt.Errorf("failed to create audioresample: %v", err)
		}

		// Add audio caps filter
		audioCaps := gst.NewCapsFromString("audio/x-raw,rate=44100,channels=2")
		audioCapsFilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
			"caps": audioCaps,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create audiocaps: %v", err)
		}

		// Create a queue before the audio encoder
		audioQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
			"max-size-buffers": 100,
			"max-size-time":    uint64(500 * time.Millisecond),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create audioQueue: %v", err)
		}

		// Create AAC encoder
		aacenc, err := gst.NewElementWithProperties("avenc_aac", map[string]interface{}{
			"bitrate": 128000, // 128kbps audio
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create aacenc: %v", err)
		}

		// Create MPEG-TS muxer
		// Create MPEG-TS muxer
		mpegtsmux, err := gst.NewElementWithProperties("mpegtsmux", map[string]interface{}{
			"alignment":    7,                    // GST_MPEG_TS_MUX_ALIGNMENT_ALIGNED
			"pat-interval": int64(100 * 1000000), // 100ms
			"pmt-interval": int64(100 * 1000000), // 100ms
			"pcr-interval": int64(20 * 1000000),  // 20ms
			"latency":      0,                    // Minimize muxer latency
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create mpegtsmux: %v", err)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create sync identity: %v", err)
		}

		// Create a tee element to split the stream for multiple outputs
		tee, err := gst.NewElement("tee")
		if err != nil {
			return nil, fmt.Errorf("failed to create tee element: %v", err)
		}

		rtpQueue, err := gst.NewElement("queue")
		if err != nil {
			return nil, fmt.Errorf("could not create RTP queue: %v", err)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create rtpQueue: %v", err)
		}

		// Create RTP payloader for MPEG-TS
		rtpmp2tpay, err := gst.NewElementWithProperties("rtpmp2tpay", map[string]interface{}{
			"pt":               33,   // Payload type for MPEG-TS
			"mtu":              1400, // Standard MTU size for RTP
			"ssrc":             0,    // Let GStreamer generate a random SSRC
			"timestamp-offset": 0,    // Let GStreamer generate a random timestamp offset
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create rtpmp2tpay: %v", err)
		}

		// Create UDP sink with optimized settings for VLC compatibility
		sinkProps := map[string]interface{}{
			"host":         s.host,
			"port":         s.port,
			"max-lateness": -1,      // Don't drop late buffers
			"ts-offset":    0,       // No timestamp offset
			"sync":         true,    // Sync to the clock for better timing
			"async":        false,   // Don't use async buffering
			"buffer-size":  1048576, // Larger buffer (1MB) for stability
		}

		// Add multicast-specific settings if needed
		isMulticast := net.ParseIP(s.host).IsMulticast()
		if isMulticast {
			sinkProps["auto-multicast"] = true
			sinkProps["ttl"] = 64 // Standard TTL for multicast

			// Don't specify a specific interface to avoid SO_BINDTODEVICE warnings
			// Instead, use the system's default routing
			fmt.Printf("Multicast stream detected, using default interface\n")
		}

		udpsink, err := gst.NewElementWithProperties("udpsink", sinkProps)
		if err != nil {
			return nil, fmt.Errorf("could not create udpsink: %v", err)
		}

		// Create debug branch elements (optional)
		debugEnabled := false // Set to true to enable debug output
		var debugQueue *gst.Element
		var debugFileSink *gst.Element

		// Add all elements to pipeline
		pipeline.AddMany(videoIdentity, videoConvert, videoScale, videoCapsFilter, h264enc, h264parse)
		pipeline.AddMany(audioIdentity, audioConvert, audioResample, audioCapsFilter, audioQueue, aacenc)
		pipeline.AddMany(mpegtsmux, tee, rtpQueue, rtpmp2tpay, udpsink)

		// Add debug elements if enabled
		if debugEnabled {
			pipeline.AddMany(debugQueue, debugFileSink)
		}

		// Connect to demuxer's pad-added signal
		decodebin.Connect("pad-added", func(element *gst.Element, pad *gst.Pad) {
			caps := pad.CurrentCaps()
			if caps == nil {
				fmt.Errorf("Warning: pad %s has no caps", pad.GetName())
				return
			}

			structure := caps.GetStructureAt(0)
			name := structure.Name()

			fmt.Errorf("Pad added with caps: %s", name)

			if len(name) >= 5 && name[:5] == "video" {
				// Link video path
				sinkpad := videoIdentity.GetStaticPad("sink")
				if sinkpad == nil {
					fmt.Errorf("Error: couldn't get sink pad from videoIdentity")
					return
				}

				if pad.Link(sinkpad) != gst.PadLinkOK {
					fmt.Errorf("Error: couldn't link video pad")
					return
				}

				fmt.Errorf("Linked video pad successfully")

				// Link the rest of the video path
				videoIdentity.Link(videoConvert)
				videoConvert.Link(videoScale)
				videoScale.Link(videoCapsFilter)
				videoCapsFilter.Link(h264enc)
				h264enc.Link(h264parse)
				h264parse.Link(mpegtsmux)

			} else if len(name) >= 5 && name[:5] == "audio" {
				// Link audio path
				sinkpad := audioIdentity.GetStaticPad("sink")
				if sinkpad == nil {
					fmt.Errorf("Error: couldn't get sink pad from audioIdentity")
					return
				}

				if pad.Link(sinkpad) != gst.PadLinkOK {
					fmt.Errorf("Error: couldn't link audio pad")
					return
				}

				fmt.Errorf("Linked audio pad successfully")

				// Link the rest of the audio path
				audioIdentity.Link(audioConvert)
				audioConvert.Link(audioResample)
				audioResample.Link(audioCapsFilter)
				audioCapsFilter.Link(audioQueue)
				audioQueue.Link(aacenc)
				aacenc.Link(mpegtsmux)
			}
		})

		mpegtsmux.Link(tee)

		teeSrcPad := tee.GetRequestPad("src_%u")
		if teeSrcPad == nil {
			fmt.Errorf("failed to get source pad from tee")
		}

		// Get the sink pad from the rtpQueue
		queueSinkPad := rtpQueue.GetStaticPad("sink")
		if queueSinkPad == nil {
			fmt.Errorf("failed to get sink pad from rtpQueue")
		}

		// Link the tee to the queue

		rtpQueue.Link(rtpmp2tpay)
		// Link muxer to final elements
		rtpmp2tpay.Link(udpsink)
		linkRet := teeSrcPad.Link(queueSinkPad)
		// Set up a message handler to catch errors

		if linkRet != gst.PadLinkOK {
			fmt.Errorf("error linking tee -> rtpQueue: %v", linkRet)
		}
		bus := pipeline.GetBus()

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

				// Set timestamp offset for continuity if we have a previous timestamp
				if s.lastEndTimestamp > 0 {
					// Find the tsoffset element
					tsoffsetName := fmt.Sprintf("tsoffset-%d", i)
					tsoffset, _ := currentPipeline.GetElementByName(tsoffsetName)
					if tsoffset != nil {
						// Calculate offset based on last pipeline's end timestamp
						// This ensures timestamps continue from where the last pipeline left off
						tsoffset.SetProperty("offset", s.lastEndTimestamp)
						fmt.Printf("[%s] Set timestamp offset to %d for item %d\n",
							time.Now().Format("15:04:05.000"), s.lastEndTimestamp, i)
					} else {
						fmt.Printf("[%s] Warning: Could not find tsoffset element '%s'\n",
							time.Now().Format("15:04:05.000"), tsoffsetName)
					}
				}
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
			var lastPosition int64 = 0

			// Use wall clock as backup if pipeline position doesn't work
			for time.Since(startTime) < targetDuration*2 { // Double duration as safety
				// Query pipeline position
				ok, position := currentPipeline.QueryPosition(gst.FormatTime)
				if ok && position > 0 {
					positionNs := position
					durationNs := int64(targetDuration.Nanoseconds())
					lastPosition = positionNs // Store the last valid position

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

						// Store the last timestamp for the next pipeline
						s.mutex.Lock()
						s.lastEndTimestamp = lastPosition
						s.mutex.Unlock()

						close(durationReached)
						return
					}
				} else {
					// If we can't query position, use wall clock as fallback
					if time.Since(startTime) >= targetDuration {
						fmt.Printf("[%s] Item %d: Duration reached via wall clock\n",
							time.Now().Format("15:04:05.000"), i)

						// If we have a last position, use it
						if lastPosition > 0 {
							s.mutex.Lock()
							s.lastEndTimestamp = lastPosition
							s.mutex.Unlock()
						}

						close(durationReached)
						return
					}
				}
				time.Sleep(20 * time.Millisecond) // Check position frequently
			}

			// Fallback - if we get here, use wall clock
			fmt.Printf("[%s] Item %d: Fallback - using wall clock\n",
				time.Now().Format("15:04:05.000"), i)

			// If we have a last position, use it
			if lastPosition > 0 {
				s.mutex.Lock()
				s.lastEndTimestamp = lastPosition
				s.mutex.Unlock()
			}

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
