// ... existing code ...
package scheduler

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
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
	host       string
	port       int
	items      []StreamItem
	pipeline   *gst.Pipeline
	mutex      sync.Mutex
	stopChan   chan struct{}
	switchNext chan struct{}
	running    bool
	muxer      *gst.Element
	currentBin *Bin
	nextBin    *Bin
}

func NewStreamScheduler(host string, port int) (*StreamScheduler, error) {
	gst.Init(nil)

	return &StreamScheduler{
		host:       host,
		port:       port,
		items:      make([]StreamItem, 0),
		stopChan:   make(chan struct{}),
		switchNext: make(chan struct{}),
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

	err := s.createMainPipeline()
	if err != nil {
		return fmt.Errorf("failed to create main pipeline: %v", err)
	}

	// Set pipeline to paused state for preroll
	s.pipeline.SetState(gst.StatePaused)

	// Process each item in the schedule
	for i, item := range items {
		fmt.Printf("[%s] Processing item %d: %s\n",
			time.Now().Format("15:04:05.000"), i, item.Source)

		// Create bin for current item
		nextBin, err := s.createInputBin(item, i)
		if err != nil {
			fmt.Printf("Failed to create bin for item %d: %v\n", i, err)
			continue
		}

		s.nextBin = nextBin

		// Switch to the newly created bin
		err = s.switchBin()
		if err != nil {
			fmt.Printf("Failed to switch to bin for item %d: %v\n", i, err)
			continue
		}

		// Set pipeline to playing state
		err = s.pipeline.SetState(gst.StatePlaying)
		if err != nil {
			fmt.Printf("Failed to set pipeline to playing for item %d: %v\n", i, err)
			continue
		}

		// Handle seeking to offset if needed
		if item.Offset > 0 {
			go func(offset time.Duration) {
				// Wait a moment for pipeline to stabilize
				time.Sleep(200 * time.Millisecond)
				fmt.Printf("[%s] Seeking to offset %v\n",
					time.Now().Format("15:04:05.000"), offset)

				// Perform seek operation with appropriate flags
				ret := s.pipeline.SeekTime(
					offset,
					gst.SeekFlagFlush|gst.SeekFlagKeyUnit,
				)
				if !ret {
					fmt.Printf("[%s] Failed to seek to offset\n",
						time.Now().Format("15:04:05.000"))
				}
			}(item.Offset)
		} else {
			// Seek to beginning to ensure proper start
			go func() {
				time.Sleep(200 * time.Millisecond)
				ret := s.pipeline.SeekDefault(0.0, gst.SeekFlagFlush|gst.SeekFlagAccurate)
				if !ret {
					fmt.Printf("[%s] Failed to seek to beginning\n",
						time.Now().Format("15:04:05.000"))
				}
			}()
		}

		fmt.Printf("[%s] Item %d: Started playing\n",
			time.Now().Format("15:04:05.000"), i)

		// Prepare next item in advance if available
		if i+1 < len(items) {
			go func(nextItem StreamItem, nextIndex int) {
				fmt.Printf("[%s] Preparing next item %d in advance\n",
					time.Now().Format("15:04:05.000"), nextIndex)

				preparedBin, err := s.createInputBin(nextItem, nextIndex)
				if err != nil {
					fmt.Printf("Failed to prepare bin for item %d: %v\n", nextIndex, err)
					return
				}

				s.mutex.Lock()
				s.nextBin = preparedBin
				s.mutex.Unlock()

				fmt.Printf("[%s] Next item %d prepared successfully\n",
					time.Now().Format("15:04:05.000"), nextIndex)
			}(items[i+1], i+1)
		}

		// Handle scheduled breaks
		if item.NeedBreak {
			go func(duration time.Duration) {
				fmt.Printf("[%s] Scheduled break set for %v\n",
					time.Now().Format("15:04:05.000"), duration)
				time.Sleep(duration)
				s.switchNext <- struct{}{}
			}(item.Duration)
		}

		// Wait for item to finish or receive control signals
		select {
		case <-time.After(item.Duration):
			fmt.Printf("[%s] Item %d: Duration reached\n",
				time.Now().Format("15:04:05.000"), i)

		case <-s.switchNext:
			fmt.Printf("[%s] Item %d: Received switch signal during playback\n",
				time.Now().Format("15:04:05.000"), i)

		case <-s.stopChan:
			fmt.Printf("[%s] Item %d: Received stop signal during playback\n",
				time.Now().Format("15:04:05.000"), i)

			s.pipeline.SetState(gst.StateNull)
			return nil
		}
	}

	s.mutex.Lock()
	s.running = false
	s.mutex.Unlock()

	s.pipeline.SetState(gst.StateNull)

	return nil
}

func (s *StreamScheduler) createMainPipeline() error {
	pipeline, err := gst.NewPipeline("streaming-pipeline")
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %v", err)
	}

	s.muxer, err = gst.NewElementWithProperties("mpegtsmux", map[string]interface{}{
		"alignment":    7,
		"pat-interval": int64(100 * 1000000),
		"pmt-interval": int64(100 * 1000000),
		"pcr-interval": int64(20 * 1000000),
		"latency":      0,
	})
	if err != nil {
		return fmt.Errorf("failed to create mpegtsmux: %v", err)
	}

	tee, err := gst.NewElement("tee")
	if err != nil {
		return fmt.Errorf("failed to create tee element: %v", err)
	}

	rtpQueue, err := gst.NewElement("queue")
	if err != nil {
		return fmt.Errorf("could not create RTP queue: %v", err)
	}

	rtpmp2tpay, err := gst.NewElementWithProperties("rtpmp2tpay", map[string]interface{}{
		"pt":               33,
		"mtu":              1400,
		"ssrc":             uint32(0),
		"timestamp-offset": uint32(0),
		"perfect-rtptime":  true,
		"seqnum-offset":    uint32(0),
	})
	if err != nil {
		return fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}

	sinkProps := map[string]interface{}{
		"host":         s.host,
		"port":         s.port,
		"max-lateness": -1,
		"ts-offset":    0,
		"sync":         true,
		"async":        false,
		"buffer-size":  1048576,
	}

	isMulticast := net.ParseIP(s.host).IsMulticast()
	if isMulticast {
		sinkProps["auto-multicast"] = true
		sinkProps["ttl"] = 64

		fmt.Printf("Multicast stream detected, using default interface\n")
	}

	udpsink, err := gst.NewElementWithProperties("udpsink", sinkProps)
	if err != nil {
		return fmt.Errorf("could not create udpsink: %v", err)
	}

	debugEnabled := false
	var debugQueue *gst.Element
	var debugFileSink *gst.Element

	pipeline.AddMany(tee, s.muxer, rtpQueue, rtpmp2tpay, udpsink)

	if debugEnabled {
		pipeline.AddMany(debugQueue, debugFileSink)
	}

	s.muxer.Link(tee)

	teeSrcPad := tee.GetRequestPad("src_%u")
	if teeSrcPad == nil {
		return fmt.Errorf("failed to get request pad from tee")
	}

	rtpQueueSinkPad := rtpQueue.GetStaticPad("sink")
	if rtpQueueSinkPad == nil {
		return fmt.Errorf("failed to get sink pad from rtpQueue")
	}

	if teeSrcPad.Link(rtpQueueSinkPad) != gst.PadLinkOK {
		return fmt.Errorf("failed to link tee to rtpQueue")
	}

	rtpQueue.Link(rtpmp2tpay)
	rtpmp2tpay.Link(udpsink)

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
				fmt.Printf("[%s] End of stream received from %s - ignoring to continue playback\n",
					time.Now().Format("15:04:05.000"), msg.Source())
			}
		}
	}()

	fmt.Println("Setting pipeline to PAUSED state for preroll...")

	pipeline.SetState(gst.StatePaused)

	fmt.Printf("[%s] Pipeline for item %d prerolled successfully\n",
		time.Now().Format("15:04:05.000"), 0)

	s.pipeline = pipeline
	return nil
}

func (s *StreamScheduler) createInputBin(item StreamItem, index int) (*Bin, error) {
	fmt.Printf("[%s] Preparing pipeline for item %d: %s\n",
		time.Now().Format("15:04:05.000"), index, item.Source)

	// Create a unique bin name with timestamp to avoid conflicts
	binName := fmt.Sprintf("input-bin-%d-%d", index, time.Now().UnixNano())
	bin := gst.NewBin(binName)

	// Create file source element
	filesrc, err := gst.NewElementWithProperties("filesrc", map[string]interface{}{
		"location": item.Source,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create filesrc: %v", err)
	}

	// Create decodebin for handling various formats
	decodebin, err := gst.NewElement("decodebin")
	if err != nil {
		return nil, fmt.Errorf("failed to create decodebin: %v", err)
	}

	// Create video processing elements
	videoQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue: %v", err)
	}

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

	videoEncoder, err := gst.NewElementWithProperties("x264enc", map[string]interface{}{
		"tune":    0x00000004, // zerolatency
		"bitrate": 2000,       // 2 Mbps
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create h264enc: %v", err)
	}

	videoCaps := gst.NewCapsFromString("video/x-h264,profile=baseline")
	videoCapsFilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": videoCaps,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create video caps filter: %v", err)
	}

	// Create audio processing elements
	audioQueue, err := gst.NewElement("queue")
	if err != nil {
		return nil, fmt.Errorf("failed to create audioQueue: %v", err)
	}

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

	audioEncoder, err := gst.NewElementWithProperties("avenc_aac", map[string]interface{}{
		"bitrate": 128000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aacenc: %v", err)
	}

	audioCaps := gst.NewCapsFromString("audio/x-raw,rate=44100,channels=2")
	audioCapsFilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"caps": audioCaps,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audio caps filter: %v", err)
	}

	// Add all elements to bin
	err = bin.AddMany(filesrc, decodebin)
	if err != nil {
		return nil, fmt.Errorf("error adding file and decode bin: %v", err)
	}

	err = bin.AddMany(videoQueue, videoIdentity, videoConvert, videoEncoder, videoCapsFilter)
	if err != nil {
		return nil, fmt.Errorf("error adding video elements: %v", err)
	}

	err = bin.AddMany(audioQueue, audioIdentity, audioConvert, audioResample, audioCapsFilter, audioEncoder)
	if err != nil {
		return nil, fmt.Errorf("error adding audio elements: %v", err)
	}

	// Link source to decodebin
	err = filesrc.Link(decodebin)
	if err != nil {
		return nil, fmt.Errorf("error linking filesrc -> decodebin: %v", err)
	}

	// Connect to pad-added signal to handle dynamic pad creation
	_, err = decodebin.Connect("pad-added", func(db *gst.Element, pad *gst.Pad) {
		caps := pad.GetCurrentCaps()
		if caps == nil {
			return
		}
		mediaType := caps.GetStructureAt(0).Name()
		if mediaType[:5] == "video" {
			// Link video elements in sequence
			err = gst.ElementLinkMany(videoQueue, videoIdentity, videoConvert, videoEncoder, videoCapsFilter, s.muxer)
			if err != nil {
				fmt.Printf("Error linking video elements: %v\n", err)
				return
			}
			sinkPad := videoQueue.GetStaticPad("sink")
			if sinkPad == nil {
				fmt.Printf("Error: couldn't get sink pad from videoQueue\n")
				return
			}
			if pad.Link(sinkPad) != gst.PadLinkOK {
				fmt.Printf("Error: couldn't link video pad\n")
				return
			}
			fmt.Printf("Linked video pad successfully\n")
		} else if mediaType[:5] == "audio" {
			// Link audio elements in sequence
			err = gst.ElementLinkMany(audioQueue, audioIdentity, audioConvert, audioResample, audioCapsFilter, audioEncoder, s.muxer)
			if err != nil {
				fmt.Printf("Error linking audio elements: %v\n", err)
				return
			}
			sinkPad := audioQueue.GetStaticPad("sink")
			if sinkPad == nil {
				fmt.Printf("Error: couldn't get sink pad from audioQueue\n")
				return
			}
			if pad.Link(sinkPad) != gst.PadLinkOK {
				fmt.Printf("Error: couldn't link audio pad\n")
				return
			}
			fmt.Printf("Linked audio pad successfully\n")
		}
	})

	if err != nil {
		return nil, fmt.Errorf("error connecting decodebin: %v", err)
	}

	return &Bin{
		Bin:        bin,
		VideoQueue: videoQueue,
		AudioQueue: audioQueue,
		Item:       item,
	}, nil
}

func (s *StreamScheduler) switchBin() error {
	if s.nextBin == nil {
		return fmt.Errorf("no next bin available to switch to")
	}

	fmt.Printf("[%s] Switching to bin for item: %s\n",
		time.Now().Format("15:04:05.000"), s.nextBin.Item.Source)

	// If we have a current bin, properly clean it up
	if s.currentBin != nil {
		fmt.Printf("[%s] Cleaning up current bin\n", time.Now().Format("15:04:05.000"))

		// First set state to NULL to release resources
		err := s.currentBin.Bin.SetState(gst.StateNull)
		if err != nil {
			fmt.Printf("Warning: error setting current bin state to null: %v\n", err)
			// Continue anyway as we need to switch bins
		}

		// Wait for state change to complete
		time.Sleep(100 * time.Millisecond)
		state, _ := s.currentBin.Bin.GetState(gst.StateNull, gst.ClockTime(3*time.Second))
		fmt.Printf("[%s] Current bin state: %v\n",
			time.Now().Format("15:04:05.000"), state)

		// Then remove from pipeline
		err = s.pipeline.Remove(s.currentBin.Bin.Element)
		if err != nil {
			fmt.Printf("Warning: error removing current bin: %v\n", err)
			// Continue anyway as we need to switch bins
		}
	}

	// Switch to the next bin
	s.currentBin = s.nextBin
	s.nextBin = nil

	fmt.Printf("[%s] Adding new bin to pipeline\n", time.Now().Format("15:04:05.000"))

	// Add the new bin to the pipeline
	err := s.pipeline.Add(s.currentBin.Bin.Element)
	if err != nil {
		return fmt.Errorf("error adding bin to pipeline: %v", err)
	}

	// Set the bin state to ready first
	fmt.Printf("[%s] Setting bin state to READY\n", time.Now().Format("15:04:05.000"))
	err = s.currentBin.Bin.SetState(gst.StateReady)
	if err != nil {
		fmt.Printf("Warning: error setting bin state to ready: %v\n", err)
		// Continue anyway and try to set to paused
	}

	// Wait for state change to complete
	time.Sleep(100 * time.Millisecond)
	state, _ := s.currentBin.Bin.GetState(gst.StateReady, gst.ClockTime(3*time.Second))
	fmt.Printf("[%s] Bin state after READY: %v\n",
		time.Now().Format("15:04:05.000"), state)

	// Set to paused first to ensure preroll
	fmt.Printf("[%s] Setting bin state to PAUSED\n", time.Now().Format("15:04:05.000"))
	err = s.currentBin.Bin.SetState(gst.StatePaused)
	if err != nil {
		fmt.Printf("Warning: error setting bin state to paused: %v\n", err)
		// Continue anyway and try to set to playing
	}

	// Wait for state change to complete
	time.Sleep(100 * time.Millisecond)
	pausedState, _ := s.currentBin.Bin.GetState(gst.StatePaused, gst.ClockTime(3*time.Second))
	fmt.Printf("[%s] Bin state after PAUSED: %v\n",
		time.Now().Format("15:04:05.000"), pausedState)

	// Then set to playing
	fmt.Printf("[%s] Setting bin state to PLAYING\n", time.Now().Format("15:04:05.000"))
	err = s.currentBin.Bin.SetState(gst.StatePlaying)
	if err != nil {
		return fmt.Errorf("error setting bin state to playing: %v", err)
	}

	// Wait for state change to complete
	time.Sleep(100 * time.Millisecond)
	playingState, _ := s.currentBin.Bin.GetState(gst.StatePlaying, gst.ClockTime(3*time.Second))
	fmt.Printf("[%s] Bin state after PLAYING: %v\n",
		time.Now().Format("15:04:05.000"), playingState)

	fmt.Printf("[%s] Successfully switched to bin for item: %s\n",
		time.Now().Format("15:04:05.000"), s.currentBin.Item.Source)

	return nil
}

func (s *StreamScheduler) Stop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.running {
		return
	}

	close(s.stopChan)
	s.running = false

	if s.pipeline != nil {
		s.pipeline.SetState(gst.StateNull)
		s.pipeline = nil
	}
}
