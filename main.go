package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-gst/go-gst/gst"
)

func main() {
	// Initialize GStreamer
	gst.Init(nil)

	// Define command line flags
	inputHost := flag.String("input-host", "239.9.9.9", "Input RTP stream host")
	inputPort := flag.Int("input-port", 5000, "Input RTP stream port")
	outputHost := flag.String("output-host", "239.8.8.8", "Output RTP stream host")
	outputPort := flag.Int("output-port", 6000, "Output RTP stream port")
	assetPath := flag.String("asset", "/home/fstar/work/video-scheduler-gstreamer/videos/input_cut.mp4", "Path to local asset video file")
	hlslink := flag.String("hlslink", "https://1404062696.rsc.cdn77.org/HLS/FIDO_SCTE.m3u8", "Output RTP stream host")
	flag.Parse()

	// Validate asset file exists
	if _, err := os.Stat(*assetPath); os.IsNotExist(err) {
		log.Fatalf("Asset file does not exist: %s", *assetPath)
	}

	fmt.Printf("Starting GStreamer Pipeline with Compositor and Audio Mixer\n")
	fmt.Printf("Input: %s:%d\n", *inputHost, *inputPort)
	fmt.Printf("Output: %s:%d\n", *outputHost, *outputPort)
	fmt.Printf("Asset: %s\n", *assetPath)
	fmt.Printf("Pipeline will switch to asset after 1 minute\n")


	// Create a channel to handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the pipeline in a goroutine
	go func() {
		err := RunHLSGStreamerPipeline(*hlslink, *outputHost, *outputPort, *assetPath)
		if err != nil {
			fmt.Printf("Pipeline error: %v", err)
		}
	}()

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("\nReceived shutdown signal, stopping pipeline...")
}
