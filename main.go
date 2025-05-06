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

	// Create a new stream scheduler with output to localhost:5000
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
		Source:   "videos/output.mp4", // First video file
		Start:    now,
		Duration: 10 * time.Second,
	})

	streamScheduler.AddItem(scheduler.StreamItem{
		Type:     "file",
		Source:   "videos/output2.mp4", // Second video file
		Start:    now.Add(10 * time.Second),
		Duration: 60 * time.Second,
	})

	// Start the scheduler
	if err := streamScheduler.RunSchedule(); err != nil {
		log.Fatalf("Failed to start scheduler: %v", err)
	}

	// Direct UDP URL for VLC
	fmt.Printf("To play in VLC: Open VLC and go to Media > Open Network Stream > enter udp://@%s:%d\n",
		"239.1.1.2", 9000)

	fmt.Println("Scheduler started. Press Ctrl+C to exit.")
	fmt.Println("Schedule:")
	fmt.Printf("0-10s: videos/output.mp4\n")
	fmt.Printf("10-70s: videos/output2.mp4\n")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	// Stop the scheduler
	streamScheduler.Stop()
	fmt.Println("Scheduler stopped.")
}
