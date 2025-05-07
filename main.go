package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-gst/go-gst/gst"

	"scheduler-rtp/scheduler"
)

func main() {
	// Initialize GStreamer
	gst.Init(nil)

	// Create a new stream scheduler with output to multicast address
	streamScheduler, err := scheduler.NewStreamScheduler("239.1.1.2", 9000)
	if err != nil {
		log.Fatalf("Failed to create scheduler: %v", err)
	}

	// Get current time to schedule items relative to now
	now := time.Now()

	// Add test file items
	// We'll schedule a test pattern for 10 seconds, then a file for 10 seconds, then back to test pattern
	streamScheduler.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "/home/ubuntu/Dev/video-scheduler-gstreamer/videos/output.mp4", // First video file
		Start:    now,
		Duration: 30 * time.Second,
	})

	streamScheduler.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "/home/ubuntu/Dev/video-scheduler-gstreamer/videos/output3.mp4", // Second video file
		Start:    now.Add(30 * time.Second),
		Duration: 30 * time.Second,
	})

	// Start the scheduler
	if err := streamScheduler.RunSchedule(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	// Direct UDP URL for VLC
	fmt.Printf("To play in VLC: Open VLC and go to Media > Open Network Stream > enter udp://@%s:%d\n",
		"239.1.1.2", 9000)

	fmt.Println("Scheduler started. Press Ctrl+C to exit.")

	// Create a second stream scheduler with output to a different multicast address
	streamScheduler1, err := scheduler.NewStreamScheduler("239.1.1.3", 9001)
	if err != nil {
		log.Fatalf("Failed to create scheduler1: %v", err)
	}

	// Add test file items for the second scheduler
	streamScheduler1.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "/home/ubuntu/Dev/video-scheduler-gstreamer/videos/output3.mp4", // First video file
		Start:    now,
		Duration: 30 * time.Second,
	})

	streamScheduler1.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "/home/ubuntu/Dev/video-scheduler-gstreamer/videos/output.mp4", // Second video file
		Start:    now.Add(30 * time.Second),
		Duration: 30 * time.Second,
	})

	// Start the second scheduler
	if err := streamScheduler1.RunSchedule(); err != nil {
		log.Fatalf("Failed to start scheduler1: %v", err)
	}

	// Direct UDP URL for VLC
	fmt.Printf("To play second scheduler in VLC: Open VLC and go to Media > Open Network Stream > enter udp://@%s:%d\n",
		"239.1.1.3", 9001)

	fmt.Println("Scheduler1 started. Press Ctrl+C to exit.")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Stop both schedulers
	streamScheduler.Stop()
	streamScheduler1.Stop()
	fmt.Println("Schedulers stopped.")
}
