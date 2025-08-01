package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/go-gst/go-gst/gst"
)

// Global variables for PTS tracking and SCTE-35 handling (HLS version)
var (
	hlsCurrentPTS      uint64 = 0
	hlsCurrentPTSMutex sync.Mutex
	hlsGlobalSCTE35Msg *HLSSCTE35Message
	hlsScte35Mutex     sync.Mutex
)

// Setup logging and panic recovery
func init() {
	// Set up logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(os.Stdout)

	// Set up panic recovery
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC RECOVERED: %v", r)
			log.Printf("Stack trace: %s", debug.Stack())
			os.Exit(1)
		}
	}()
}

// HLSSCTE35Message represents a parsed SCTE-35 message for HLS
type HLSSCTE35Message struct {
	TableID                uint8
	SectionSyntaxIndicator bool
	PrivateIndicator       bool
	SectionLength          uint16
	ProtocolVersion        uint8
	EncryptedPacket        bool
	EncryptionAlgorithm    uint8
	PTSAdjustment          uint64
	CWIndex                uint8
	Tier                   uint16
	SpliceCommandLength    uint16
	SpliceCommandType      uint8
	SpliceTime             uint64 // PTS time when splice should occur
}

// HLSGStreamerPipeline represents a GStreamer pipeline for HLS stream processing with videomixer and audio mixer
type HLSGStreamerPipeline struct {
	pipeline       *gst.Pipeline
	demux          *gst.Element
	mux            *gst.Element
	videomixer     *gst.Element
	audiomixer     *gst.Element
	rtpVolume      *gst.Element // Volume control for HLS audio
	assetVolume    *gst.Element // Volume control for asset audio
	assetPipeline  *gst.Pipeline
	mutex          sync.Mutex
	padMutex       sync.Mutex // Mutex for protecting pad operations
	assetVideoPath string
	currentInput   string // "hls" or "asset"
	assetPlaying   bool
	stopChan       chan struct{}
	running        bool
	hlsConnected   bool        // Track if HLS stream is connected
	hlsTimeout     *time.Timer // Timeout for HLS connection
	pipelineID     string      // Add this field
}

// NewHLSGStreamerPipeline creates a new GStreamer pipeline for HLS or SRT processing with videomixer and audio mixer
func NewHLSGStreamerPipeline(inputType string, inputUrl string, outputHost string, outputPort int, assetVideoPath string) (*HLSGStreamerPipeline, error) {
	// Generate a unique pipeline ID
	pipelineID := fmt.Sprintf("pipeline_%d", time.Now().UnixNano())

	// Create pipeline with unique name
	pipeline, err := gst.NewPipeline(fmt.Sprintf("hls-pipeline-%s", pipelineID))
	if err != nil {
		return nil, fmt.Errorf("failed to create pipeline: %v", err)
	}

	// Create the pipeline instance
	gp := &HLSGStreamerPipeline{
		pipeline:       pipeline,
		assetVideoPath: assetVideoPath,
		currentInput:   "hls",
		assetPlaying:   false,
		stopChan:       make(chan struct{}),
		running:        false,
		hlsConnected:   false,
		hlsTimeout:     time.NewTimer(30 * time.Second),
		pipelineID:     pipelineID,
	}

	var srcElement *gst.Element
	var demuxElement *gst.Element

	if inputType == "srt" {
		srcElement, err = gst.NewElementWithProperties("srtsrc", map[string]interface{}{
			"uri":                 inputUrl,
			"latency":             200,
			"timeout":             uint64(10 * 1000000000),
			"wait-for-connection": true,
			"do-timestamp":        true,
			"name":                fmt.Sprintf("srtSrc_%s", pipelineID),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create SRT source: %v", err)
		}
		demuxElement, err = gst.NewElementWithProperties("tsdemux", map[string]interface{}{
			"name":               fmt.Sprintf("tsdemux_%s", pipelineID),
			"send-scte35-events": true,
			"latency":            1000,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create tsdemux: %v", err)
		}
	} else {
		srcElement, err = gst.NewElementWithProperties("souphttpsrc", map[string]interface{}{
			"name": fmt.Sprintf("hlsSrc_%s", pipelineID),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create hls source: %v", err)
		}
		demuxElement, err = gst.NewElementWithProperties("hlsdemux", map[string]interface{}{
			"name": fmt.Sprintf("hlsDemux_%s", pipelineID),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create hls demuxer: %v", err)
		}
	}

	// Add src and demux to pipeline
	if err := pipeline.Add(srcElement); err != nil {
		return nil, fmt.Errorf("failed to add srcElement to pipeline: %v", err)
	}
	if err := pipeline.Add(demuxElement); err != nil {
		return nil, fmt.Errorf("failed to add demuxElement to pipeline: %v", err)
	}

	// Link src to demux
	if err := srcElement.Link(demuxElement); err != nil {
		return nil, fmt.Errorf("failed to link srcElement to demuxElement: %v", err)
	}

	// Create the pipeline instance
	gp = &HLSGStreamerPipeline{
		pipeline:       pipeline,
		assetVideoPath: assetVideoPath,
		currentInput:   "hls",
		assetPlaying:   false,
		stopChan:       make(chan struct{}),
		running:        false,
		hlsConnected:   false,
		hlsTimeout:     time.NewTimer(30 * time.Second),
		pipelineID:     pipelineID,
	}

	// Create intervideosink for HLS stream (input1)
	intervideosink1, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel":      "input1",
		"sync":         false,
		"name":         fmt.Sprintf("intervideosink1_%s", pipelineID),
		"max-lateness": int64(20 * 1000000),
		"qos":          true,
		"async":        false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosink1: %v", err)
	}

	// Create interaudiosink for HLS stream (audio1)
	interaudiosink1, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel":      "audio1",
		"sync":         false,
		"name":         fmt.Sprintf("interaudiosink1_%s", pipelineID),
		"max-lateness": int64(20 * 1000000),
		"qos":          true,
		"async":        false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosink1: %v", err)
	}

	// Create intervideosrc for HLS stream (input1)
	intervideo1, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input1",
		"do-timestamp": true,
		"name":         fmt.Sprintf("intervideosrc1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosrc1: %v", err)
	}

	// Create intervideosrc for asset stream (input2)
	intervideo2, err := gst.NewElementWithProperties("intervideosrc", map[string]interface{}{
		"channel":      "input2",
		"do-timestamp": true,
		"name":         fmt.Sprintf("intervideosrc2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create intervideosrc2: %v", err)
	}

	// Create interaudiosrc for HLS stream (audio1)
	interaudio1, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio1",
		"do-timestamp": true,
		"name":         fmt.Sprintf("interaudiosrc1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosrc1: %v", err)
	}

	// Create interaudiosrc for asset stream (audio2)
	interaudio2, err := gst.NewElementWithProperties("interaudiosrc", map[string]interface{}{
		"channel":      "audio2",
		"do-timestamp": true,
		"name":         fmt.Sprintf("interaudiosrc2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create interaudiosrc2: %v", err)
	}

	// Create video mixer
	videomixer, err := gst.NewElementWithProperties("videomixer", map[string]interface{}{
		"name": fmt.Sprintf("videomixer_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create videomixer: %v", err)
	}

	// Create audio mixer
	audiomixer, err := gst.NewElementWithProperties("audiomixer", map[string]interface{}{
		"name": fmt.Sprintf("audiomixer_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audiomixer: %v", err)
	}

	// Audio mixer settings optimized for seamless audio
	audiomixer.SetProperty("latency", int64(200*1000000)) // Reduced from 100ms to 200ms for better sync
	audiomixer.SetProperty("silent", false)
	audiomixer.SetProperty("resampler", "audioconvert")
	// Add audio sync properties to prevent cutting
	audiomixer.SetProperty("sync", true)
	audiomixer.SetProperty("async", false)
	audiomixer.SetProperty("max-lateness", int64(50*1000000)) // 50ms max lateness
	audiomixer.SetProperty("qos", true)

	// Create volume control for HLS audio (input1)
	hlsVolume, err := gst.NewElementWithProperties("volume", map[string]interface{}{
		"name": fmt.Sprintf("hlsVolume_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create HLS volume control: %v", err)
	}
	hlsVolume.SetProperty("volume", 1.0)

	// Create volume control for asset audio (input2)
	assetVolume, err := gst.NewElementWithProperties("volume", map[string]interface{}{
		"name": fmt.Sprintf("assetVolume_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create asset volume control: %v", err)
	}
	assetVolume.SetProperty("volume", 0.0)

	// Create audio format converters for dynamic audio formats
	hlsAudioConvert, err := gst.NewElementWithProperties("audioconvert", map[string]interface{}{
		"name": fmt.Sprintf("hlsAudioConvert_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create HLS audio convert: %v", err)
	}
	hlsAudioConvert.SetProperty("sync", true)

	assetAudioConvert, err := gst.NewElementWithProperties("audioconvert", map[string]interface{}{
		"name": fmt.Sprintf("assetAudioConvert_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create asset audio convert: %v", err)
	}
	assetAudioConvert.SetProperty("sync", true)

	// Create audio capsfilters to ensure compatible formats
	hlsAudioCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"name": fmt.Sprintf("hlsAudioCapsfilter_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create HLS audio capsfilter: %v", err)
	}
	// Set flexible audio caps that can handle various formats
	hlsAudioCapsfilter.SetProperty("caps", gst.NewCapsFromString("audio/x-raw,format={S16LE,S32LE,F32LE},layout=interleaved,rate={44100,48000},channels={1,2}"))

	assetAudioCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"name": fmt.Sprintf("assetAudioCapsfilter_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create asset audio capsfilter: %v", err)
	}
	// Set flexible audio caps that can handle various formats
	assetAudioCapsfilter.SetProperty("caps", gst.NewCapsFromString("audio/x-raw,format={S16LE,S32LE,F32LE},layout=interleaved,rate={44100,48000},channels={1,2}"))

	// Create video processing elements for HLS stream (input1)
	videoQueue1, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("videoQueue1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue 1: %v", err)
	}
	// Optimize for seamless HLS playback
	videoQueue1.SetProperty("max-size-buffers", 50)                   // Reduced from 100
	videoQueue1.SetProperty("max-size-time", uint64(500*1000000))     // Reduced from 700ms to 500ms
	videoQueue1.SetProperty("min-threshold-time", uint64(20*1000000)) // Reduced from 50ms to 20ms
	videoQueue1.SetProperty("sync", false)
	videoQueue1.SetProperty("leaky", 2) // Leak downstream (newer frames)
	videoQueue1.SetProperty("max-size-bytes", 0)
	videoQueue1.SetProperty("silent", false)

	// Create video processing elements for asset stream (input2)
	videoQueue2, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("videoQueue2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create video queue 2: %v", err)
	}
	// Optimize for seamless HLS playback
	videoQueue2.SetProperty("max-size-buffers", 50)                   // Reduced from 100
	videoQueue2.SetProperty("max-size-time", uint64(500*1000000))     // Reduced from 700ms to 500ms
	videoQueue2.SetProperty("min-threshold-time", uint64(20*1000000)) // Reduced from 50ms to 20ms
	videoQueue2.SetProperty("sync", false)
	videoQueue2.SetProperty("leaky", 2) // Leak downstream (newer frames)
	videoQueue2.SetProperty("max-size-bytes", 0)
	videoQueue2.SetProperty("silent", false)

	// Create video processing chain elements
	videoconvert, err := gst.NewElementWithProperties("videoconvert", map[string]interface{}{
		"name": fmt.Sprintf("videoconvert_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create videoconvert: %v", err)
	}

	// Create capsfilter before videomixer
	videomixerCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"name": fmt.Sprintf("videomixerCapsfilter_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create videomixer capsfilter: %v", err)
	}
	videomixerCapsfilter.SetProperty("caps", gst.NewCapsFromString("video/x-raw"))

	// Create capsfilter after videomixer
	videomixerOutputCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
		"name": fmt.Sprintf("videomixerOutputCapsfilter_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create videomixer output capsfilter: %v", err)
	}
	videomixerOutputCapsfilter.SetProperty("caps", gst.NewCapsFromString("video/x-raw"))

	x264enc, err := gst.NewElementWithProperties("x264enc", map[string]interface{}{
		"name": fmt.Sprintf("x264enc_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create x264enc: %v", err)
	}

	h264parse2, err := gst.NewElementWithProperties("h264parse", map[string]interface{}{
		"name": fmt.Sprintf("h264parse2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create h264parse2: %v", err)
	}

	// Video queue before muxer
	videoMuxerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("videoMuxerQueue_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create video muxer queue: %v", err)
	}
	videoMuxerQueue.SetProperty("max-size-buffers", 500)
	videoMuxerQueue.SetProperty("max-size-time", uint64(500*1000000))
	videoMuxerQueue.SetProperty("min-threshold-time", uint64(50*1000000))
	videoMuxerQueue.SetProperty("sync", false)
	videoMuxerQueue.SetProperty("leaky", 0)
	videoMuxerQueue.SetProperty("max-size-bytes", 0)

	// Create audio processing elements for HLS stream (audio1)
	audioQueue1, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("audioQueue1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audio queue 1: %v", err)
	}
	// Optimize for seamless HLS playback and prevent audio cutting
	audioQueue1.SetProperty("max-size-buffers", 2000)                  // Increased from 500 to prevent underruns
	audioQueue1.SetProperty("max-size-time", uint64(3000*1000000))     // Increased from 1000ms to 3000ms
	audioQueue1.SetProperty("min-threshold-time", uint64(500*1000000)) // Increased from 200ms to 500ms
	audioQueue1.SetProperty("sync", true)                              // Enable sync for audio
	audioQueue1.SetProperty("leaky", 0)                                // Don't leak audio frames
	audioQueue1.SetProperty("max-size-bytes", 0)
	audioQueue1.SetProperty("silent", false)

	// Create audio processing elements for asset stream (audio2)
	audioQueue2, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("audioQueue2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audio queue 2: %v", err)
	}
	// Optimize for seamless HLS playback and prevent audio cutting
	audioQueue2.SetProperty("max-size-buffers", 2000)                  // Increased from 500 to prevent underruns
	audioQueue2.SetProperty("max-size-time", uint64(3000*1000000))     // Increased from 1000ms to 3000ms
	audioQueue2.SetProperty("min-threshold-time", uint64(500*1000000)) // Increased from 200ms to 500ms
	audioQueue2.SetProperty("sync", true)                              // Enable sync for audio
	audioQueue2.SetProperty("leaky", 0)                                // Don't leak audio frames
	audioQueue2.SetProperty("max-size-bytes", 0)
	audioQueue2.SetProperty("silent", false)

	// Create audio processing chain elements
	aacparse1, err := gst.NewElementWithProperties("aacparse", map[string]interface{}{
		"name": fmt.Sprintf("aacparse1_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aacparse1: %v", err)
	}
	aacparse1.SetProperty("sync", true) // Enable sync for audio parsing

	audioconvert, err := gst.NewElementWithProperties("audioconvert", map[string]interface{}{
		"name": fmt.Sprintf("audioconvert_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audioconvert: %v", err)
	}
	audioconvert.SetProperty("sync", true) // Enable sync for audio conversion

	audioresample, err := gst.NewElementWithProperties("audioresample", map[string]interface{}{
		"name": fmt.Sprintf("audioresample_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audioresample: %v", err)
	}
	audioresample.SetProperty("sync", true) // Enable sync for audio resampling

	voaacenc, err := gst.NewElementWithProperties("avenc_aac", map[string]interface{}{
		"name": fmt.Sprintf("voaacenc_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create voaacenc: %v", err)
	}
	voaacenc.SetProperty("sync", true) // Enable sync for audio encoding

	aacparse2, err := gst.NewElementWithProperties("aacparse", map[string]interface{}{
		"name": fmt.Sprintf("aacparse2_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aacparse2: %v", err)
	}
	aacparse2.SetProperty("sync", true) // Enable sync for audio parsing

	// Audio queue before muxer
	audioMuxerQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
		"name": fmt.Sprintf("audioMuxerQueue_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create audio muxer queue: %v", err)
	}
	// Optimize for seamless audio and prevent cutting
	audioMuxerQueue.SetProperty("max-size-buffers", 4000)                   // Increased from 2000 to prevent underruns
	audioMuxerQueue.SetProperty("max-size-time", uint64(5000*1000000))      // Increased from 3000ms to 5000ms
	audioMuxerQueue.SetProperty("min-threshold-time", uint64(1000*1000000)) // Increased from 1000ms to 1000ms
	audioMuxerQueue.SetProperty("sync", true)                               // Enable sync for audio
	audioMuxerQueue.SetProperty("leaky", 0)                                 // Don't leak audio frames
	audioMuxerQueue.SetProperty("max-size-bytes", 0)
	audioMuxerQueue.SetProperty("silent", false)

	// Muxer and output elements
	mpegtsmux, err := gst.NewElementWithProperties("mpegtsmux", map[string]interface{}{
		"name": fmt.Sprintf("mpegtsmux_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create mpegtsmux: %v", err)
	}

	rtpmp2tpay, err := gst.NewElementWithProperties("rtpmp2tpay", map[string]interface{}{
		"name": fmt.Sprintf("rtpmp2tpay_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpmp2tpay: %v", err)
	}

	rtpbin, err := gst.NewElementWithProperties("rtpbin", map[string]interface{}{
		"latency":                uint(200), // Reduced from 400ms to 200ms for lower latency
		"do-retransmission":      true,
		"rtp-profile":            2,
		"ntp-sync":               true,
		"ntp-time-source":        3,
		"max-rtcp-rtp-time-diff": 1000,
		"max-dropout-time":       45000,
		"max-misorder-time":      5000,
		"buffer-mode":            1,
		"do-lost":                true,
		"rtcp-sync-send-time":    true,
		"name":                   fmt.Sprintf("rtpbin_%s", pipelineID),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create rtpbin: %v", err)
	}

	udpsink, err := gst.NewElementWithProperties("udpsink", map[string]interface{}{
		"host":           outputHost,
		"port":           outputPort,
		"sync":           false,   // Keep false for UDP output
		"buffer-size":    1048576, // Increased from 524288 to 1MB for better buffering
		"auto-multicast": true,
		"name":           fmt.Sprintf("udpsink_%s", pipelineID),
		"async":          false, // Disable async for more reliable output
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create udpsink: %v", err)
	}

	// Set HLS source properties
	srcElement.SetProperty("location", inputUrl)
	srcElement.SetProperty("timeout", uint64(10*1000000000)) // 10 second timeout
	srcElement.SetProperty("retries", 3)                     // Retry 3 times
	srcElement.SetProperty("user-id", "")
	srcElement.SetProperty("user-pw", "")
	srcElement.SetProperty("do-timestamp", true)
	srcElement.SetProperty("http-log-level", 3)
	// Configure HLS demuxer for seamless playback
	demuxElement.SetProperty("timeout", uint64(5*1000000000)) // Reduced to 5 second timeout
	demuxElement.SetProperty("max-errors", 5)                 // Increased max errors
	demuxElement.SetProperty("async", false)
	demuxElement.SetProperty("sync", false)
	// Add segment handling optimizations
	demuxElement.SetProperty("segment-duration", uint64(2*1000000000)) // 2 second segments
	demuxElement.SetProperty("segment-start-time", int64(0))
	demuxElement.SetProperty("segment-stop-time", int64(-1)) // -1 means until end
	demuxElement.SetProperty("segment-repeat", 0)            // No repeat
	demuxElement.SetProperty("segment-offset", int64(0))
	demuxElement.SetProperty("segment-trickmode-interval", uint64(0)) // No trick mode

	// Configure pipeline latency
	pipeline.SetProperty("latency", int64(200*1000000)) // Reduced from 500ms to 200ms for lower latency
	x264enc.SetProperty("tune", "zerolatency")
	// Add HLS-specific encoder optimizations
	x264enc.SetProperty("speed-preset", "ultrafast") // Fastest encoding for low latency
	x264enc.SetProperty("key-int-max", 30)           // Keyframe interval for HLS segments
	x264enc.SetProperty("bframes", 0)                // No B-frames for lower latency
	x264enc.SetProperty("ref", 1)                    // Single reference frame for lower latency

	// Audio encoder properties
	voaacenc.SetProperty("bitrate", 192000) // Increased from 128000 for better audio quality
	voaacenc.SetProperty("channels", 2)
	// Add audio encoder optimizations to prevent cutting
	voaacenc.SetProperty("quality", 2)        // High quality encoding
	voaacenc.SetProperty("profile", 2)        // AAC-LC profile for better compatibility
	voaacenc.SetProperty("afterburner", true) // Enable afterburner for better quality

	// Audio resampler properties
	audioresample.SetProperty("quality", 10)
	audioresample.SetProperty("in-samplerate", 48000)
	audioresample.SetProperty("out-samplerate", 48000)
	audioresample.SetProperty("filter-mode", 0)

	// AAC parser properties
	aacparse1.SetProperty("outputformat", 0)
	aacparse2.SetProperty("outputformat", 0)

	// Configure MPEG-TS muxer
	mpegtsmux.SetProperty("alignment", 7)
	mpegtsmux.SetProperty("pat-interval", int64(27000*1000000))
	mpegtsmux.SetProperty("pmt-interval", int64(27000*1000000))
	mpegtsmux.SetProperty("pcr-interval", int64(2700*1000000))
	mpegtsmux.SetProperty("start-time", int64(500000000))
	mpegtsmux.SetProperty("si-interval", int64(500))

	// Configure RTP payloader
	rtpmp2tpay.SetProperty("mtu", 1400)
	rtpmp2tpay.SetProperty("pt", 33)
	rtpmp2tpay.SetProperty("perfect-rtptime", true)

	// Add elements to pipeline
	elements := []*gst.Element{
		srcElement, demuxElement, demuxElement,
		intervideosink1, interaudiosink1,
		intervideo1, intervideo2, videomixer,
		interaudio1, interaudio2, audiomixer,
		hlsVolume, assetVolume,
		hlsAudioConvert, assetAudioConvert, hlsAudioCapsfilter, assetAudioCapsfilter,
		videoQueue1, videoQueue2, videoconvert, videomixerCapsfilter, videomixerOutputCapsfilter, x264enc, h264parse2, videoMuxerQueue,
		audioQueue1, audioQueue2, aacparse1, audioconvert, audioresample, voaacenc, aacparse2,
		audioMuxerQueue, mpegtsmux, rtpmp2tpay,
		rtpbin, udpsink,
	}

	for _, element := range elements {
		if err := pipeline.Add(element); err != nil {
			return nil, fmt.Errorf("failed to add element to pipeline: %v", err)
		}
	}

	// Link elements (except demuxers which need dynamic linking)
	if err := srcElement.Link(demuxElement); err != nil {
		return nil, fmt.Errorf("failed to link srcElement to demuxElement: %v", err)
	}

	// Link video elements directly
	if err := intervideo1.Link(videoQueue1); err != nil {
		return nil, fmt.Errorf("failed to link intervideo1 to videoQueue1: %v", err)
	}
	if err := intervideo2.Link(videoQueue2); err != nil {
		return nil, fmt.Errorf("failed to link intervideo2 to videoQueue2: %v", err)
	}

	// Link video processing chain
	if err := videoQueue1.Link(videomixer); err != nil {
		return nil, fmt.Errorf("failed to link videoQueue1 to videomixer: %v", err)
	}
	if err := videoQueue2.Link(videomixer); err != nil {
		return nil, fmt.Errorf("failed to link videoQueue2 to videomixer: %v", err)
	}
	if err := videomixer.Link(videoconvert); err != nil {
		return nil, fmt.Errorf("failed to link videomixer to videoconvert: %v", err)
	}
	if err := videoconvert.Link(videomixerCapsfilter); err != nil {
		return nil, fmt.Errorf("failed to link videoconvert to videomixer capsfilter: %v", err)
	}
	if err := videomixerCapsfilter.Link(x264enc); err != nil {
		return nil, fmt.Errorf("failed to link videomixer capsfilter to x264enc: %v", err)
	}
	if err := x264enc.Link(h264parse2); err != nil {
		return nil, fmt.Errorf("failed to link x264enc to h264parse2: %v", err)
	}
	if err := h264parse2.Link(videoMuxerQueue); err != nil {
		return nil, fmt.Errorf("failed to link h264parse2 to videoMuxerQueue: %v", err)
	}
	if err := videoMuxerQueue.Link(mpegtsmux); err != nil {
		return nil, fmt.Errorf("failed to link videoMuxerQueue to mpegtsmux: %v", err)
	}

	// Link audio elements directly
	if err := interaudio1.Link(audioQueue1); err != nil {
		return nil, fmt.Errorf("failed to link interaudio1 to audioQueue1: %v", err)
	}
	if err := interaudio2.Link(audioQueue2); err != nil {
		return nil, fmt.Errorf("failed to link interaudio2 to audioQueue2: %v", err)
	}

	// Link audio processing chain with volume controls and format conversion
	if err := audioQueue1.Link(hlsAudioConvert); err != nil {
		return nil, fmt.Errorf("failed to link audioQueue1 to hlsAudioConvert: %v", err)
	}
	if err := hlsAudioConvert.Link(hlsAudioCapsfilter); err != nil {
		return nil, fmt.Errorf("failed to link hlsAudioConvert to hlsAudioCapsfilter: %v", err)
	}
	if err := hlsAudioCapsfilter.Link(hlsVolume); err != nil {
		return nil, fmt.Errorf("failed to link hlsAudioCapsfilter to hlsVolume: %v", err)
	}
	if err := hlsVolume.Link(audiomixer); err != nil {
		return nil, fmt.Errorf("failed to link hlsVolume to audiomixer: %v", err)
	}
	if err := audioQueue2.Link(assetAudioConvert); err != nil {
		return nil, fmt.Errorf("failed to link audioQueue2 to assetAudioConvert: %v", err)
	}
	if err := assetAudioConvert.Link(assetAudioCapsfilter); err != nil {
		return nil, fmt.Errorf("failed to link assetAudioConvert to assetAudioCapsfilter: %v", err)
	}
	if err := assetAudioCapsfilter.Link(assetVolume); err != nil {
		return nil, fmt.Errorf("failed to link assetAudioCapsfilter to assetVolume: %v", err)
	}
	if err := assetVolume.Link(audiomixer); err != nil {
		return nil, fmt.Errorf("failed to link assetVolume to audiomixer: %v", err)
	}
	if err := audiomixer.Link(audioconvert); err != nil {
		return nil, fmt.Errorf("failed to link audiomixer to audioconvert: %v", err)
	}
	if err := audioconvert.Link(audioresample); err != nil {
		return nil, fmt.Errorf("failed to link audioconvert to audioresample: %v", err)
	}
	if err := audioresample.Link(voaacenc); err != nil {
		return nil, fmt.Errorf("failed to link audioresample to voaacenc: %v", err)
	}
	if err := voaacenc.Link(aacparse2); err != nil {
		return nil, fmt.Errorf("failed to link voaacenc to aacparse2: %v", err)
	}
	if err := aacparse2.Link(audioMuxerQueue); err != nil {
		return nil, fmt.Errorf("failed to link aacparse2 to audioMuxerQueue: %v", err)
	}
	if err := audioMuxerQueue.Link(mpegtsmux); err != nil {
		return nil, fmt.Errorf("failed to link audioMuxerQueue to mpegtsmux: %v", err)
	}

	// Link output branch
	if err := mpegtsmux.Link(rtpmp2tpay); err != nil {
		return nil, fmt.Errorf("failed to link mpegtsmux to rtpmp2tpay: %v", err)
	}

	sendRtpSinkPad := rtpbin.GetRequestPad("send_rtp_sink_0")
	if sendRtpSinkPad == nil {
		return nil, fmt.Errorf("could not get 'send_rtp_sink_0' pad from rtpbin")
	}

	rtpmp2tpaySrcPad := rtpmp2tpay.GetStaticPad("src")
	if rtpmp2tpaySrcPad == nil {
		return nil, fmt.Errorf("could not get 'src' pad from rtpmp2tpay")
	}
	if rtpmp2tpaySrcPad.Link(sendRtpSinkPad) != gst.PadLinkOK {
		return nil, fmt.Errorf("failed to link rtpmp2tpay src to rtpbin send_rtp_sink_0")
	}

	rtpSrcPad := rtpbin.GetStaticPad("send_rtp_src_0")
	if rtpSrcPad == nil {
		return nil, fmt.Errorf("could not get 'send_rtp_src_0' pad from rtpbin")
	}
	if rtpSrcPad.Link(udpsink.GetStaticPad("sink")) != gst.PadLinkOK {
		return nil, fmt.Errorf("failed to link rtpbin send_rtp_src_0 to udpsink")
	}

	demuxElement.Connect("pad-added", func(self *gst.Element, pad *gst.Pad) {
		fmt.Printf("[%s] HLS demuxer pad added: %s\n", pipelineID, pad.GetName())

		// Check if pad is already linked
		if pad.IsLinked() {
			fmt.Printf("[%s] Pad %s is already linked, skipping\n", pipelineID, pad.GetName())
			return
		}

		// Print pad caps for debugging
		padCaps := pad.GetCurrentCaps()
		if padCaps != nil {
			fmt.Printf("[%s] HLS demuxer pad caps: %v\n", pipelineID, padCaps.String())
		} else {
			fmt.Printf("[%s] HLS demuxer pad has no caps yet\n", pipelineID)
		}

		// Add a small delay to ensure TS demuxer is ready
		go func() {
			time.Sleep(50 * time.Millisecond) // Reduced from 100ms to 50ms for faster segment transitions

			// Link HLS demuxer output to tsdemux
			tsdemuxSinkPad := demuxElement.GetStaticPad("sink")
			if tsdemuxSinkPad == nil {
				fmt.Printf("[%s] TS demuxer sink pad is nil\n", pipelineID)
				return
			}

			// Check if TS demuxer sink pad is already linked
			if tsdemuxSinkPad.IsLinked() {
				fmt.Printf("[%s] TS demuxer sink pad is already linked\n", pipelineID)
				return
			}

			// Check TS demuxer state
			tsdemuxState := demuxElement.GetCurrentState()
			fmt.Printf("[%s] TS demuxer state before linking: %s\n", pipelineID, tsdemuxState.String())

			linkResult := pad.Link(tsdemuxSinkPad)
			if linkResult != gst.PadLinkOK {
				fmt.Printf("[%s] Failed to link HLS demuxer to tsdemux: %v\n", pipelineID, linkResult)
				fmt.Printf("[%s] HLS pad caps: %v\n", pipelineID, pad.GetCurrentCaps())
				fmt.Printf("[%s] TS demuxer sink pad caps: %v\n", pipelineID, tsdemuxSinkPad.GetCurrentCaps())

				// Try to set TS demuxer to PLAYING state and retry
				if tsdemuxState != gst.StatePlaying {
					fmt.Printf("[%s] Setting TS demuxer to PLAYING state and retrying...\n", pipelineID)
					if err := demuxElement.SetState(gst.StatePlaying); err != nil {
						fmt.Printf("[%s] Failed to set TS demuxer to PLAYING: %v\n", pipelineID, err)
						return
					}
					time.Sleep(25 * time.Millisecond) // Reduced from 50ms to 25ms

					linkResult = pad.Link(tsdemuxSinkPad)
					if linkResult != gst.PadLinkOK {
						fmt.Printf("[%s] Retry failed to link HLS demuxer to tsdemux: %v\n", pipelineID, linkResult)
					} else {
						fmt.Printf("[%s] Successfully linked HLS demuxer to tsdemux on retry\n", pipelineID)
					}
				}
			} else {
				fmt.Printf("[%s] Successfully linked HLS demuxer to tsdemux\n", pipelineID)
			}
		}()
	})

	// Set up dynamic pad-added signal for tsdemux
	demuxElement.Connect("pad-added", func(self *gst.Element, pad *gst.Pad) {
		fmt.Printf("[%s] ##############################################\n", pipelineID)

		go func() {
			time.Sleep(10 * time.Millisecond)

			gp.padMutex.Lock()
			defer gp.padMutex.Unlock()

			if !gp.running {
				fmt.Printf("[%s] Pipeline is not running, ignoring pad addition\n", pipelineID)
				return
			}

			if pad == nil {
				fmt.Printf("[%s] Warning: Received nil pad in pad-added signal\n", pipelineID)
				return
			}

			padName := pad.GetName()
			if padName == "" {
				fmt.Printf("[%s] Warning: Received pad with empty name\n", pipelineID)
				return
			}

			fmt.Printf("[%s] Demuxer pad added: %s\n", pipelineID, padName)

			// Check pad name to determine if it's video or audio
			if len(padName) >= 5 && padName[:5] == "video" {
				fmt.Printf("[%s] Creating video pipeline for HLS stream\n", pipelineID)

				// Check if video elements already exist for this pad
				elementName := fmt.Sprintf("videoDemuxQueue_%s_%s", pipelineID, padName)
				existing, err := gp.pipeline.GetElementByName(elementName)
				if err == nil && existing != nil {
					fmt.Printf("[%s] Video elements for pad %s already exist, skipping creation\n", pipelineID, padName)
					return
				}

				// Create video processing chain similar to RTP version
				videoDemuxQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
					"name": elementName,
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video demux queue: %v\n", pipelineID, err)
					return
				}

				// Print pad caps for debugging
				padCaps := pad.GetCurrentCaps()
				if padCaps != nil {
					fmt.Printf("[%s] Video pad caps: %v\n", pipelineID, padCaps.String())
				} else {
					fmt.Printf("[%s] Video pad has no caps yet\n", pipelineID)
				}

				// 1. Add to pipeline
				if err := gp.pipeline.Add(videoDemuxQueue); err != nil {
					fmt.Printf("[%s] Failed to add videoDemuxQueue to pipeline: %v\n", pipelineID, err)
					return
				}
				// 2. Set state to READY
				if err := videoDemuxQueue.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoDemuxQueue to PLAYING: %v\n", pipelineID, err)
					return
				}
				// Optimize for seamless HLS segment transitions
				videoDemuxQueue.SetProperty("max-size-buffers", 200)                  // Reduced from 500
				videoDemuxQueue.SetProperty("max-size-time", uint64(500*1000000))     // Reduced from 1000ms to 500ms
				videoDemuxQueue.SetProperty("min-threshold-time", uint64(50*1000000)) // Reduced from 200ms to 50ms
				videoDemuxQueue.SetProperty("sync", false)
				videoDemuxQueue.SetProperty("leaky", 2) // Leak downstream (newer frames)
				videoDemuxQueue.SetProperty("max-size-bytes", 0)
				videoDemuxQueue.SetProperty("silent", false)

				// Create video processing elements
				videoInputCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("videoInputCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video input capsfilter: %v\n", pipelineID, err)
					return
				}
				videoInputCapsfilter.SetProperty("caps", gst.NewCapsFromString("video/x-h264"))

				// Add videoInputCapsfilter to pipeline
				if err := gp.pipeline.Add(videoInputCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add videoInputCapsfilter to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := videoInputCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoInputCapsfilter to PLAYING: %v\n", pipelineID, err)
					return
				}

				videoH264parse, err := gst.NewElementWithProperties("h264parse", map[string]interface{}{
					"name": fmt.Sprintf("videoH264parse_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video h264parse: %v\n", pipelineID, err)
					return
				}
				videoH264parse.SetProperty("config-interval", -1)
				videoH264parse.SetProperty("disable-passthrough", false)
				videoH264parse.SetProperty("update-timecode", true)

				// Add videoH264parse to pipeline
				if err := gp.pipeline.Add(videoH264parse); err != nil {
					fmt.Printf("[%s] Failed to add videoH264parse to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := videoH264parse.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoH264parse to PLAYING: %v\n", pipelineID, err)
					return
				}

				videoParseQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
					"name": fmt.Sprintf("videoParseQueue_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video parse queue: %v\n", pipelineID, err)
					return
				}
				// Add videoParseQueue to pipeline
				if err := gp.pipeline.Add(videoParseQueue); err != nil {
					fmt.Printf("[%s] Failed to add videoParseQueue to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := videoParseQueue.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoParseQueue to PLAYING: %v\n", pipelineID, err)
					return
				}
				videoParseQueue.SetProperty("max-size-buffers", 100)
				videoParseQueue.SetProperty("max-size-time", uint64(500*1000000))
				videoParseQueue.SetProperty("min-threshold-time", uint64(200*1000000))
				videoParseQueue.SetProperty("sync", false)
				videoParseQueue.SetProperty("leaky", 0)

				videoCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("videoCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video capsfilter: %v\n", pipelineID, err)
					return
				}
				videoCapsfilter.SetProperty("caps", gst.NewCapsFromString("video/x-h264,alignment=au,stream-format=byte-stream"))

				// Add videoCapsfilter to pipeline
				if err := gp.pipeline.Add(videoCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add videoCapsfilter to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := videoCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoCapsfilter to PLAYING: %v\n", pipelineID, err)
					return
				}

				videoOutputCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("videoOutputCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video output capsfilter: %v\n", pipelineID, err)
					return
				}
				videoOutputCapsfilter.SetProperty("caps", gst.NewCapsFromString("video/x-raw"))

				// Add videoOutputCapsfilter to pipeline
				if err := gp.pipeline.Add(videoOutputCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add videoOutputCapsfilter to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := videoOutputCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoOutputCapsfilter to PLAYING: %v\n", pipelineID, err)
					return
				}

				videoDecoder, err := gst.NewElementWithProperties("avdec_h264", map[string]interface{}{
					"name": fmt.Sprintf("videoDecoder_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video decoder: %v\n", pipelineID, err)
					return
				}
				videoDecoder.SetProperty("sync", false)

				// Add videoDecoder to pipeline
				if err := gp.pipeline.Add(videoDecoder); err != nil {
					fmt.Printf("[%s] Failed to add videoDecoder to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := videoDecoder.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoDecoder to PLAYING: %v\n", pipelineID, err)
					return
				}

				videoConvert, err := gst.NewElementWithProperties("videoconvert", map[string]interface{}{
					"name": fmt.Sprintf("videoConvert_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video convert: %v\n", pipelineID, err)
					return
				}
				videoConvert.SetProperty("sync", false)

				// Add videoConvert to pipeline
				if err := gp.pipeline.Add(videoConvert); err != nil {
					fmt.Printf("[%s] Failed to add videoConvert to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := videoConvert.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoConvert to PLAYING: %v\n", pipelineID, err)
					return
				}

				videoScale, err := gst.NewElementWithProperties("videoscale", map[string]interface{}{
					"name": fmt.Sprintf("videoScale_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create video scale: %v\n", pipelineID, err)
					return
				}
				videoScale.SetProperty("sync", false)
				videoScale.SetProperty("method", 0)

				// Add videoScale to pipeline
				if err := gp.pipeline.Add(videoScale); err != nil {
					fmt.Printf("[%s] Failed to add videoScale to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := videoScale.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set videoScale to PLAYING: %v\n", pipelineID, err)
					return
				}

				// Link the video chain with better error handling
				fmt.Printf("[%s] Starting video chain linking...\n", pipelineID)

				// Link pad to videoDemuxQueue
				sinkPad := videoDemuxQueue.GetStaticPad("sink")
				if sinkPad == nil {
					fmt.Printf("[%s] videoDemuxQueue sink pad is nil\n", pipelineID)
					return
				}

				// Check if pad is already linked
				if pad.IsLinked() {
					fmt.Printf("[%s] Video pad %s is already linked, skipping\n", pipelineID, padName)
					return
				}

				linkResult := pad.Link(sinkPad)
				if linkResult != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link video pad to queue: %v\n", pipelineID, linkResult)
					fmt.Printf("[%s] Pad caps: %v\n", pipelineID, pad.GetCurrentCaps())
					fmt.Printf("[%s] Sink pad caps: %v\n", pipelineID, sinkPad.GetCurrentCaps())
					return
				}
				fmt.Printf("[%s] Successfully linked video pad to queue\n", pipelineID)

				// Before linking, check for nil and already-linked pads, and print caps if linking fails
				srcPad := videoDemuxQueue.GetStaticPad("src")
				sinkPad = videoInputCapsfilter.GetStaticPad("sink")
				if srcPad == nil {
					fmt.Printf("[%s] videoDemuxQueue src pad is nil\n", pipelineID)
					return
				}
				if sinkPad == nil {
					fmt.Printf("[%s] videoInputCapsfilter sink pad is nil\n", pipelineID)
					return
				}
				if srcPad.IsLinked() {
					fmt.Printf("[%s] videoDemuxQueue src pad is already linked\n", pipelineID)
					return
				}
				if sinkPad.IsLinked() {
					fmt.Printf("[%s] videoInputCapsfilter sink pad is already linked\n", pipelineID)
					return
				}
				linkResult = srcPad.Link(sinkPad)
				if linkResult != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link queue to input capsfilter: %v\n", pipelineID, linkResult)
					fmt.Printf("[%s] queue src pad caps: %v\n", pipelineID, srcPad.GetCurrentCaps())
					fmt.Printf("[%s] capsfilter sink pad caps: %v\n", pipelineID, sinkPad.GetCurrentCaps())
					return
				}
				fmt.Printf("[%s] Successfully linked queue to input capsfilter\n", pipelineID)

				if videoInputCapsfilter.GetStaticPad("src").Link(videoH264parse.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link input capsfilter to h264parse\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked input capsfilter to h264parse\n", pipelineID)

				if videoH264parse.GetStaticPad("src").Link(videoParseQueue.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link h264parse to parse queue\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked h264parse to parse queue\n", pipelineID)

				if videoParseQueue.GetStaticPad("src").Link(videoCapsfilter.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link parse queue to capsfilter\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked parse queue to capsfilter\n", pipelineID)

				if videoCapsfilter.GetStaticPad("src").Link(videoDecoder.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link capsfilter to decoder\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked capsfilter to decoder\n", pipelineID)

				if videoDecoder.GetStaticPad("src").Link(videoConvert.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link decoder to convert\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked decoder to convert\n", pipelineID)

				if videoConvert.GetStaticPad("src").Link(videoScale.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link convert to scale\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked convert to scale\n", pipelineID)

				if videoScale.GetStaticPad("src").Link(videoOutputCapsfilter.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link scale to output capsfilter\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked scale to output capsfilter\n", pipelineID)

				if videoOutputCapsfilter.GetStaticPad("src").Link(intervideosink1.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link output capsfilter to intervideosink\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked video pipeline for HLS stream\n", pipelineID)

				// Force videomixer refresh
				go func() {
					fmt.Printf("[%s] Forcing videomixer refresh to prevent black screen\n", pipelineID)
					pad1 := gp.videomixer.GetStaticPad("sink_0")
					pad2 := gp.videomixer.GetStaticPad("sink_1")
					if pad1 != nil && pad2 != nil {
						pad1.SetProperty("alpha", 1.0)
						pad2.SetProperty("alpha", 0.0)
						fmt.Printf("[%s] Switched videomixer to HLS content\n", gp.pipelineID)
					}
					fmt.Printf("[%s] Videomixer refresh completed\n", pipelineID)
				}()

				// Mark HLS as connected
				if !gp.hlsConnected {
					gp.hlsConnected = true
					gp.hlsTimeout.Stop()
					fmt.Printf("[%s] HLS stream connected successfully - Video pipeline should be active\n", pipelineID)
				}

				// Add PTS probe for SCTE-35 handling
				pad := videoDecoder.GetStaticPad("src")
				pad.AddProbe(gst.PadProbeTypeBuffer, func(pad *gst.Pad, info *gst.PadProbeInfo) gst.PadProbeReturn {
					buffer := info.GetBuffer()
					if buffer != nil {
						hlsCurrentPTSMutex.Lock()
						hlsCurrentPTS = uint64(buffer.PresentationTimestamp())
						hlsCurrentPTSMutex.Unlock()

						hlsScte35Mutex.Lock()
						if hlsGlobalSCTE35Msg != nil && hlsGlobalSCTE35Msg.SpliceTime > hlsCurrentPTS {
							hlsGlobalSCTE35Msg.SpliceTime = math.MaxUint64
							fmt.Printf("[%s] SCTE-35 splice time (%d) > current PTS (%d), switching to asset\n",
								pipelineID, hlsGlobalSCTE35Msg.SpliceTime, hlsCurrentPTS)
							hlsScte35Mutex.Unlock()
							// gp.switchToAsset()
							return gst.PadProbeOK
						}
						hlsScte35Mutex.Unlock()
					}
					return gst.PadProbeOK
				})

			} else if len(padName) >= 5 && padName[:5] == "audio" {
				fmt.Printf("[%s] Creating audio pipeline for HLS stream\n", pipelineID)

				// Create audio processing elements
				audioDemuxQueue, err := gst.NewElementWithProperties("queue", map[string]interface{}{
					"name": fmt.Sprintf("audioDemuxQueue_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio demux queue: %v\n", pipelineID, err)
					return
				}
				// Optimize for seamless HLS segment transitions and prevent audio cutting
				audioDemuxQueue.SetProperty("max-size-buffers", 1500)                  // Increased from 750 to prevent underruns
				audioDemuxQueue.SetProperty("max-size-time", uint64(2500*1000000))     // Increased from 1500ms to 2500ms
				audioDemuxQueue.SetProperty("min-threshold-time", uint64(500*1000000)) // Increased from 300ms to 500ms
				audioDemuxQueue.SetProperty("sync", true)                              // Enable sync for audio
				audioDemuxQueue.SetProperty("leaky", 0)                                // Don't leak audio frames
				audioDemuxQueue.SetProperty("max-size-bytes", 0)
				audioDemuxQueue.SetProperty("silent", false)

				// Add audioDemuxQueue to pipeline
				if err := gp.pipeline.Add(audioDemuxQueue); err != nil {
					fmt.Printf("[%s] Failed to add audioDemuxQueue to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := audioDemuxQueue.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audioDemuxQueue to PLAYING: %v\n", pipelineID, err)
					return
				}

				audioAacparse, err := gst.NewElementWithProperties("aacparse", map[string]interface{}{
					"name": fmt.Sprintf("audioAacparse_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio aacparse: %v\n", pipelineID, err)
					return
				}
				audioAacparse.SetProperty("sync", false)

				// Add audioAacparse to pipeline
				if err := gp.pipeline.Add(audioAacparse); err != nil {
					fmt.Printf("[%s] Failed to add audioAacparse to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := audioAacparse.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audioAacparse to PLAYING: %v\n", pipelineID, err)
					return
				}

				audioDecoder, err := gst.NewElementWithProperties("avdec_aac", map[string]interface{}{
					"name": fmt.Sprintf("audioDecoder_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio decoder: %v\n", pipelineID, err)
					return
				}
				audioDecoder.SetProperty("sync", false)

				// Add audioDecoder to pipeline
				if err := gp.pipeline.Add(audioDecoder); err != nil {
					fmt.Printf("[%s] Failed to add audioDecoder to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := audioDecoder.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audioDecoder to PLAYING: %v\n", pipelineID, err)
					return
				}

				audioConvert, err := gst.NewElementWithProperties("audioconvert", map[string]interface{}{
					"name": fmt.Sprintf("audioConvert_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio convert: %v\n", pipelineID, err)
					return
				}
				audioConvert.SetProperty("sync", true)

				// Add audioConvert to pipeline
				if err := gp.pipeline.Add(audioConvert); err != nil {
					fmt.Printf("[%s] Failed to add audioConvert to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := audioConvert.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audioConvert to PLAYING: %v\n", pipelineID, err)
					return
				}

				audioResample, err := gst.NewElementWithProperties("audioresample", map[string]interface{}{
					"name": fmt.Sprintf("audioResample_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio resample: %v\n", pipelineID, err)
					return
				}
				audioResample.SetProperty("sync", true)
				audioResample.SetProperty("quality", 10)

				// Add audioResample to pipeline
				if err := gp.pipeline.Add(audioResample); err != nil {
					fmt.Printf("[%s] Failed to add audioResample to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := audioResample.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audioResample to PLAYING: %v\n", pipelineID, err)
					return
				}

				// Create dynamic audio capsfilter for format negotiation
				dynamicAudioCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("dynamicAudioCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create dynamic audio capsfilter: %v\n", pipelineID, err)
					return
				}
				// Set flexible audio caps for dynamic formats
				dynamicAudioCapsfilter.SetProperty("caps", gst.NewCapsFromString("audio/x-raw,format={S16LE,S32LE,F32LE},layout=interleaved,rate={44100,48000},channels={1,2}"))

				// Add dynamicAudioCapsfilter to pipeline
				if err := gp.pipeline.Add(dynamicAudioCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add dynamicAudioCapsfilter to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := dynamicAudioCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set dynamicAudioCapsfilter to PLAYING: %v\n", pipelineID, err)
					return
				}

				audioCapsfilter, err := gst.NewElementWithProperties("capsfilter", map[string]interface{}{
					"name": fmt.Sprintf("audioCapsfilter_%s_%s", pipelineID, padName),
				})
				if err != nil {
					fmt.Printf("[%s] Failed to create audio capsfilter: %v\n", pipelineID, err)
					return
				}
				audioCapsfilter.SetProperty("caps", gst.NewCapsFromString("audio/x-raw,format=S16LE,rate=48000,channels=2,layout=interleaved"))

				// Add audioCapsfilter to pipeline
				if err := gp.pipeline.Add(audioCapsfilter); err != nil {
					fmt.Printf("[%s] Failed to add audioCapsfilter to pipeline: %v\n", pipelineID, err)
					return
				}
				if err := audioCapsfilter.SetState(gst.StatePlaying); err != nil {
					fmt.Printf("[%s] Failed to set audioCapsfilter to PLAYING: %v\n", pipelineID, err)
					return
				}

				// Link the audio chain with format conversion
				if pad.Link(audioDemuxQueue.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link audio pad to queue\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked audio pad to queue\n", pipelineID)

				if audioDemuxQueue.GetStaticPad("src").Link(audioAacparse.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link queue to aacparse\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked queue to aacparse\n", pipelineID)

				if audioAacparse.GetStaticPad("src").Link(audioDecoder.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link aacparse to decoder\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked aacparse to decoder\n", pipelineID)

				if audioDecoder.GetStaticPad("src").Link(audioConvert.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link decoder to audioconvert\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked decoder to audioconvert\n", pipelineID)

				if audioConvert.GetStaticPad("src").Link(dynamicAudioCapsfilter.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link audioconvert to dynamic capsfilter\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked audioconvert to dynamic capsfilter\n", pipelineID)

				if dynamicAudioCapsfilter.GetStaticPad("src").Link(audioResample.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link dynamic capsfilter to audioresample\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked dynamic capsfilter to audioresample\n", pipelineID)

				if audioResample.GetStaticPad("src").Link(audioCapsfilter.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link audioresample to capsfilter\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked audioresample to capsfilter\n", pipelineID)

				if audioCapsfilter.GetStaticPad("src").Link(interaudiosink1.GetStaticPad("sink")) != gst.PadLinkOK {
					fmt.Printf("[%s] Failed to link capsfilter to interaudiosink\n", pipelineID)
					return
				}
				fmt.Printf("[%s] Successfully linked audio pipeline for HLS stream\n", pipelineID)

				// Mark HLS as connected
				if !gp.hlsConnected {
					gp.hlsConnected = true
					gp.hlsTimeout.Stop()
					fmt.Printf("[%s] HLS stream connected successfully\n", pipelineID)
				}

			} else {
				fmt.Printf("[%s] Unknown demuxer pad: %s\n", pipelineID, padName)
			}
		}()
	})

	// Set up bus watch for the main pipeline
	bus := pipeline.GetBus()
	go func() {
		for {
			msg := bus.TimedPop(gst.ClockTimeNone)
			if msg == nil {
				break
			}

			switch msg.Type() {
			case gst.MessageStateChanged:
				oldState, newState := msg.ParseStateChanged()
				fmt.Printf("[%s] Pipeline state changed: %s -> %s\n", pipelineID, oldState.String(), newState.String())

				if newState == gst.StatePaused && oldState == gst.StateReady {
					fmt.Printf("[%s] Pipeline stuck in PAUSED state - likely no HLS data received\n", pipelineID)
					// Add more debugging for HLS source
					if srcElement != nil {
						fmt.Printf("[%s] HLS source state: %s\n", pipelineID, srcElement.GetCurrentState().String())
					}
					if demuxElement != nil {
						fmt.Printf("[%s] HLS demuxer state: %s\n", pipelineID, demuxElement.GetCurrentState().String())
					}
					// go func() {
					// 	time.Sleep(5 * time.Second)
					// 	if gp.running && !gp.hlsConnected {
					// 		fmt.Printf("[%s] Pipeline still not receiving HLS data, switching to asset\n", pipelineID)
					// 		gp.switchToAsset()
					// 	}
					// }()
				}

				if newState == gst.StatePaused && oldState == gst.StatePlaying {
					fmt.Printf("[%s] Pipeline went from PLAYING to PAUSED - checking for HLS data issues\n", pipelineID)
					// Add debugging for element states
					if srcElement != nil {
						fmt.Printf("[%s] HLS source state: %s\n", pipelineID, srcElement.GetCurrentState().String())
					}
					if demuxElement != nil {
						fmt.Printf("[%s] HLS demuxer state: %s\n", pipelineID, demuxElement.GetCurrentState().String())
					}
					if demuxElement != nil {
						fmt.Printf("[%s] TS demuxer state: %s\n", pipelineID, demuxElement.GetCurrentState().String())
					}
					// go func() {
					// 	time.Sleep(2 * time.Second)
					// 	if gp.running && !gp.hlsConnected {
					// 		fmt.Printf("[%s] Pipeline paused due to no HLS data, switching to asset\n", pipelineID)
					// 		gp.switchToAsset()
					// 	}
					// }()
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("[%s] Pipeline error: %s\n", pipelineID, gerr.Error())

				// Add more detailed error analysis
				errorMsg := gerr.Error()
				fmt.Printf("[%s] Error details: %s\n", pipelineID, errorMsg)

				if strings.Contains(errorMsg, "not-negotiated") || strings.Contains(errorMsg, "Internal data stream error") {
					fmt.Printf("[%s] Pipeline negotiation error detected - this may be due to no HLS data or format issues\n", pipelineID)

					// Add HLS-specific debugging
					fmt.Printf("[%s] HLS connection status: %v\n", pipelineID, gp.hlsConnected)
					fmt.Printf("[%s] Pipeline running status: %v\n", pipelineID, gp.running)

					// Check HLS source and demuxer states
					if srcElement != nil {
						srcState := srcElement.GetCurrentState()
						fmt.Printf("[%s] HLS source state: %s\n", pipelineID, srcState.String())
					}
					if demuxElement != nil {
						demuxState := demuxElement.GetCurrentState()
						fmt.Printf("[%s] HLS demuxer state: %s\n", pipelineID, demuxState.String())
					}

					// go func() {
					// 	time.Sleep(3 * time.Second)
					// 	if gp.running && !gp.hlsConnected {
					// 		fmt.Printf("[%s] Still no HLS connection after negotiation error, switching to asset\n", pipelineID)
					// 		gp.switchToAsset()
					// 	}
					// }()
				} else if !gp.hlsConnected && (strings.Contains(errorMsg, "no pads") || strings.Contains(errorMsg, "not linked") || strings.Contains(errorMsg, "streaming")) {
					fmt.Printf("[%s] Pipeline error likely due to no HLS data - switching to asset\n", pipelineID)
					// gp.switchToAsset()
				} else {
					// For other errors, try to get more context
					fmt.Printf("[%s] Unknown pipeline error, checking element states...\n", pipelineID)
					if srcElement != nil {
						fmt.Printf("[%s] HLS source state: %s\n", pipelineID, srcElement.GetCurrentState().String())
					}
					if demuxElement != nil {
						fmt.Printf("[%s] HLS demuxer state: %s\n", pipelineID, demuxElement.GetCurrentState().String())
					}
					if demuxElement != nil {
						fmt.Printf("[%s] TS demuxer state: %s\n", pipelineID, demuxElement.GetCurrentState().String())
					}
				}
			case gst.MessageWarning:
				gwarn := msg.ParseWarning()
				fmt.Printf("[%s] Pipeline warning: %s\n", pipelineID, gwarn.Error())
			case gst.MessageInfo:
				ginfo := msg.ParseInfo()
				fmt.Printf("[%s] Pipeline info: %s\n", pipelineID, ginfo.Error())
			case gst.MessageElement:
				// Handle SCTE-35 events
				structure := msg.GetStructure()
				if structure != nil && structure.Name() == "scte35" {
					fmt.Printf("[%s] Received SCTE-35 event!\n", pipelineID)

					scte35Msg := gp.parseSCTE35Message(structure)
					if scte35Msg != nil {
						fmt.Printf("[%s] SCTE-35 message parsed: CommandType=%d, SpliceTime=%d\n",
							pipelineID, scte35Msg.SpliceCommandType, scte35Msg.SpliceTime)

						hlsScte35Mutex.Lock()
						hlsGlobalSCTE35Msg = scte35Msg
						hlsScte35Mutex.Unlock()

						// if scte35Msg.SpliceCommandType == 0x05 {
						// 	fmt.Printf("[%s] Immediate SCTE-35 splice insert detected, switching to asset\n", pipelineID)
						// 	gp.switchToAsset()
						// }
					}
				}
			case gst.MessageEOS:
				fmt.Printf("[%s] Pipeline reached end of stream\n", pipelineID)
			}
		}
	}()

	// Assign elements to the pipeline instance
	gp.demux = demuxElement
	gp.mux = mpegtsmux
	gp.videomixer = videomixer
	gp.audiomixer = audiomixer
	gp.rtpVolume = hlsVolume
	gp.assetVolume = assetVolume

	fmt.Printf("[%s] HLS Pipeline created successfully\n", pipelineID)
	return gp, nil
}

// createAssetPipeline creates a pipeline for playing the local asset video file
func (gp *HLSGStreamerPipeline) createAssetPipeline() error {
	pipeline, err := gst.NewPipeline(fmt.Sprintf("asset-pipeline-%s", gp.pipelineID))
	if err != nil {
		return fmt.Errorf("failed to create asset pipeline: %v", err)
	}

	playbin, err := gst.NewElementWithProperties("playbin3", map[string]interface{}{
		"uri":  fmt.Sprintf("file://%s", gp.assetVideoPath),
		"name": fmt.Sprintf("playbin_%s", gp.pipelineID),
	})
	if err != nil {
		return fmt.Errorf("failed to create playbin: %v", err)
	}

	playbin.SetProperty("async", false)
	playbin.SetProperty("sync", true)
	playbin.SetProperty("buffer-size", 1048576)
	playbin.SetProperty("buffer-duration", int64(5*1000000000))
	playbin.SetProperty("rate", 1.0)
	playbin.SetProperty("max-lateness", int64(20*1000000))
	playbin.SetProperty("qos", true)

	intervideosink, err := gst.NewElementWithProperties("intervideosink", map[string]interface{}{
		"channel":      "input2",
		"sync":         true,
		"name":         fmt.Sprintf("asset_intervideosink_%s", gp.pipelineID),
		"max-lateness": int64(20 * 1000000),
		"qos":          true,
		"async":        false,
	})
	if err != nil {
		return fmt.Errorf("failed to create intervideosink: %v", err)
	}

	interaudiosink, err := gst.NewElementWithProperties("interaudiosink", map[string]interface{}{
		"channel":      "audio2",
		"sync":         true,
		"name":         fmt.Sprintf("asset_interaudiosink_%s", gp.pipelineID),
		"max-lateness": int64(20 * 1000000),
		"qos":          true,
		"async":        false,
	})
	if err != nil {
		return fmt.Errorf("failed to create interaudiosink: %v", err)
	}

	playbin.SetProperty("video-sink", intervideosink)
	playbin.SetProperty("audio-sink", interaudiosink)

	if err := pipeline.Add(playbin); err != nil {
		return fmt.Errorf("failed to add playbin to pipeline: %v", err)
	}

	bus := pipeline.GetBus()
	go func() {
		for {
			msg := bus.TimedPop(gst.ClockTimeNone)
			if msg == nil {
				break
			}

			switch msg.Type() {
			case gst.MessageStateChanged:
				oldState, newState := msg.ParseStateChanged()
				if newState == gst.StatePlaying && oldState != gst.StatePaused {
					fmt.Printf("[%s] Asset pipeline is now PLAYING with proper timing\n", gp.pipelineID)
				}
			case gst.MessageError:
				gerr := msg.ParseError()
				fmt.Printf("[%s] Asset pipeline error: %s\n", gp.pipelineID, gerr.Error())
				gp.stopAsset()
			case gst.MessageEOS:
				fmt.Printf("[%s] Asset finished, switching back to HLS\n", gp.pipelineID)
				gp.switchToHLS()
			}
		}
	}()

	fmt.Printf("[%s] Asset pipeline created successfully with proper timing controls\n", gp.pipelineID)
	gp.assetPipeline = pipeline
	return nil
}

// switchToAsset switches the videomixer and audio mixer to show the asset video
func (gp *HLSGStreamerPipeline) switchToAsset() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if gp.currentInput == "asset" {
		fmt.Printf("[%s] Already playing asset, ignoring switch request\n", gp.pipelineID)
		return
	}

	fmt.Printf("[%s] Switching to asset video: %s\n", gp.pipelineID, gp.assetVideoPath)

	err := gp.createAssetPipeline()
	if err != nil {
		fmt.Printf("[%s] Failed to create asset pipeline: %v\n", gp.pipelineID, err)
		return
	}

	pad1 := gp.videomixer.GetStaticPad("sink_0")
	pad2 := gp.videomixer.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 0.0)
		pad2.SetProperty("alpha", 1.0)
		pad2.SetProperty("xpos", 0)
		pad2.SetProperty("ypos", 0)
		fmt.Printf("[%s] Switched videomixer to asset content with original dimensions\n", gp.pipelineID)
	}

	if err := gp.assetPipeline.SetState(gst.StatePlaying); err != nil {
		fmt.Printf("[%s] Failed to set asset pipeline state: %v\n", gp.pipelineID, err)
		return
	}

	gp.currentInput = "asset"
	gp.assetPlaying = true

	if gp.rtpVolume != nil {
		gp.rtpVolume.SetProperty("volume", 0.0)
		fmt.Printf("[%s] Muted HLS audio volume\n", gp.pipelineID)
	}
	if gp.assetVolume != nil {
		gp.assetVolume.SetProperty("volume", 1.0)
		fmt.Printf("[%s] Unmuted asset audio volume\n", gp.pipelineID)
	}
}

// switchToHLS switches the videomixer and audio mixer back to the HLS stream
func (gp *HLSGStreamerPipeline) switchToHLS() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if gp.currentInput == "hls" {
		fmt.Printf("[%s] Already playing HLS, ignoring switch request\n", gp.pipelineID)
		return
	}

	fmt.Printf("[%s] Switching back to HLS stream\n", gp.pipelineID)

	if gp.assetPipeline != nil {
		if err := gp.assetPipeline.SetState(gst.StateNull); err != nil {
			fmt.Printf("[%s] Failed to stop asset pipeline: %v\n", gp.pipelineID, err)
		}
		gp.assetPipeline = nil
	}

	gp.currentInput = "hls"
	gp.assetPlaying = false

	hlsCurrentPTSMutex.Lock()
	hlsCurrentPTS = 0
	hlsCurrentPTSMutex.Unlock()
	fmt.Printf("[%s] Reset currentPTS to 0 after playing local asset\n", gp.pipelineID)

	if gp.rtpVolume != nil {
		gp.rtpVolume.SetProperty("volume", 1.0)
		fmt.Printf("[%s] Unmuted HLS audio volume\n", gp.pipelineID)
	}
	if gp.assetVolume != nil {
		gp.assetVolume.SetProperty("volume", 0.0)
		fmt.Printf("[%s] Muted asset audio volume\n", gp.pipelineID)
	}

	gp.hlsConnected = false
	// if gp.hlsTimeout != nil {
	// 	gp.hlsTimeout.Stop()
	// }
	// gp.hlsTimeout = time.NewTimer(10 * time.Second)

	// go func() {
	// 	select {
	// 	case <-gp.hlsTimeout.C:
	// 		if gp.running && !gp.hlsConnected {
	// 			fmt.Printf("[%s] HLS connection timeout after switch back - no packets received within 10 seconds\n", gp.pipelineID)
	// 			fmt.Printf("[%s] Switching to asset video due to HLS timeout\n", gp.pipelineID)
	// 			gp.switchToAsset()
	// 		}
	// 	case <-gp.stopChan:
	// 		gp.hlsTimeout.Stop()
	// 		return
	// 	}
	// }()

	pad1 := gp.videomixer.GetStaticPad("sink_0")
	pad2 := gp.videomixer.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 1.0)
		pad2.SetProperty("alpha", 0.0)
		fmt.Printf("[%s] Switched videomixer back to HLS content\n", gp.pipelineID)
	}
}

// stopAsset stops the currently playing asset
func (gp *HLSGStreamerPipeline) stopAsset() {
	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	if !gp.assetPlaying {
		return
	}

	fmt.Printf("[%s] Stopping asset playback\n", gp.pipelineID)

	if gp.assetPipeline != nil {
		if err := gp.assetPipeline.SetState(gst.StateNull); err != nil {
			fmt.Printf("[%s] Failed to stop asset pipeline: %v\n", gp.pipelineID, err)
		}
		gp.assetPipeline = nil
	}

	gp.assetPlaying = false
	gp.currentInput = "hls"

	hlsCurrentPTSMutex.Lock()
	hlsCurrentPTS = 0
	hlsCurrentPTSMutex.Unlock()
	fmt.Printf("[%s] Reset currentPTS to 0 after stopping asset\n", gp.pipelineID)

	if gp.rtpVolume != nil {
		gp.rtpVolume.SetProperty("volume", 1.0)
		fmt.Printf("[%s] Unmuted HLS audio volume after asset stop\n", gp.pipelineID)
	}
	if gp.assetVolume != nil {
		gp.assetVolume.SetProperty("volume", 0.0)
		fmt.Printf("[%s] Muted asset audio volume after asset stop\n", gp.pipelineID)
	}

	gp.hlsConnected = false
	if gp.hlsTimeout != nil {
		gp.hlsTimeout.Stop()
	}
	gp.hlsTimeout = time.NewTimer(30 * time.Second)

	go func() {
		select {
		case <-gp.hlsTimeout.C:
			if gp.running && !gp.hlsConnected {
				fmt.Printf("[%s] HLS connection timeout after asset stop - no packets received within 30 seconds\n", gp.pipelineID)
				fmt.Printf("[%s] Switching to asset video due to HLS timeout\n", gp.pipelineID)
				gp.switchToAsset()
			}
		case <-gp.stopChan:
			gp.hlsTimeout.Stop()
			return
		}
	}()

	pad1 := gp.videomixer.GetStaticPad("sink_0")
	pad2 := gp.videomixer.GetStaticPad("sink_1")
	if pad1 != nil && pad2 != nil {
		pad1.SetProperty("alpha", 1.0)
		pad2.SetProperty("alpha", 0.0)
		fmt.Printf("[%s] Switched videomixer back to HLS content\n", gp.pipelineID)
	}
}

// Start starts the GStreamer pipeline
func (gp *HLSGStreamerPipeline) Start() error {
	if err := gp.pipeline.SetState(gst.StatePlaying); err != nil {
		return fmt.Errorf("failed to set pipeline state to playing: %v", err)
	}

	gp.running = true
	fmt.Printf("[%s] HLS GStreamer pipeline started successfully\n", gp.pipelineID)

	go func() {
		select {
		case <-gp.hlsTimeout.C:
			if gp.running && !gp.hlsConnected {
				fmt.Printf("[%s] HLS connection timeout - no packets received within 30 seconds\n", gp.pipelineID)
				fmt.Printf("[%s] Switching to asset video due to HLS timeout\n", gp.pipelineID)
				gp.switchToAsset()
			}
		case <-gp.stopChan:
			gp.hlsTimeout.Stop()
			return
		}
	}()

	// go func() {
	// 	time.Sleep(10 * time.Second)
	// 	if gp.running && gp.hlsConnected {
	// 		fmt.Printf("[%s] 10 seconds elapsed, switching to asset video\n", gp.pipelineID)
	// 		gp.switchToAsset()
	// 	}
	// }()

	return nil
}

// Stop stops the GStreamer pipeline
func (gp *HLSGStreamerPipeline) Stop() {
	fmt.Println("[", gp.pipelineID, "] Stopping HLS GStreamer pipeline...")

	gp.mutex.Lock()
	defer gp.mutex.Unlock()

	gp.running = false
	close(gp.stopChan)

	if gp.hlsTimeout != nil {
		gp.hlsTimeout.Stop()
	}

	if gp.assetPipeline != nil {
		if err := gp.assetPipeline.SetState(gst.StateNull); err != nil {
			fmt.Printf("[%s] Failed to stop asset pipeline: %v\n", gp.pipelineID, err)
		}
		gp.assetPipeline = nil
	}

	if gp.pipeline != nil {
		if err := gp.pipeline.SetState(gst.StateNull); err != nil {
			fmt.Printf("[%s] Failed to stop main pipeline: %v\n", gp.pipelineID, err)
		}
	}

	fmt.Println("[", gp.pipelineID, "] HLS GStreamer pipeline stopped")
}

// RunHLSGStreamerPipeline runs the GStreamer pipeline with the specified parameters
func RunHLSGStreamerPipeline(hlsUrl string, outputHost string, outputPort int, assetVideoPath string) error {
	fmt.Printf("Starting HLS GStreamer pipeline: %s -> %s:%d with asset: %s\n", hlsUrl, outputHost, outputPort, assetVideoPath)

	pipeline, err := NewHLSGStreamerPipeline("hls", hlsUrl, outputHost, outputPort, assetVideoPath)
	if err != nil {
		return fmt.Errorf("failed to create pipeline: %v", err)
	}

	if err := pipeline.Start(); err != nil {
		return fmt.Errorf("failed to start pipeline: %v", err)
	}

	select {}
}

// parseSCTE35Message parses a SCTE-35 message from GStreamer structure data
func (gp *HLSGStreamerPipeline) parseSCTE35Message(structure *gst.Structure) *HLSSCTE35Message {
	msg := &HLSSCTE35Message{}

	if tableID, _ := structure.GetValue("table-id"); tableID != nil {
		if val, ok := tableID.(uint8); ok {
			msg.TableID = val
		}
	}

	if sectionLength, _ := structure.GetValue("section-length"); sectionLength != nil {
		if val, ok := sectionLength.(uint16); ok {
			msg.SectionLength = val
		}
	}

	if spliceCommandType, _ := structure.GetValue("splice-command-type"); spliceCommandType != nil {
		if val, ok := spliceCommandType.(uint8); ok {
			msg.SpliceCommandType = val
		}
	}

	if ptsAdjustment, _ := structure.GetValue("pts-adjustment"); ptsAdjustment != nil {
		if val, ok := ptsAdjustment.(uint64); ok {
			msg.PTSAdjustment = val
		}
	}

	if spliceTime, _ := structure.GetValue("splice-time"); spliceTime != nil {
		if val, ok := spliceTime.(uint64); ok {
			msg.SpliceTime = val
		}
	}

	if spliceTime, _ := structure.GetValue("pts_time"); spliceTime != nil {
		if val, ok := spliceTime.(uint64); ok {
			msg.SpliceTime = val
		}
	}

	if spliceTime, _ := structure.GetValue("pts-time"); spliceTime != nil {
		if val, ok := spliceTime.(uint64); ok {
			msg.SpliceTime = val
		}
	}

	return msg
}

// hlsAbs returns the absolute value of an int64
func hlsAbs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
