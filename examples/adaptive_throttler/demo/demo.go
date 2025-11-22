package main

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

// main demonstrates adaptive throttling with simulated resource pressure.
// The demo:
// 1. Produces 250 elements in bursts
// 2. Processes elements with CPU-intensive work (50ms each)
// 3. Simulates increasing memory pressure as more elements are processed
// 4. The adaptive throttler adjusts throughput based on CPU/memory usage
// 5. Stats are logged every 500ms showing rate adaptation
func main() {
	var elementsProcessed atomic.Int64

	// Set up demo configuration with memory simulation
	throttler := setupDemoThrottler(&elementsProcessed)
	defer func() {
		throttler.Close()
	}()

	in := make(chan any)
	out := make(chan any, 32)

	source := ext.NewChanSource(in)
	sink := ext.NewChanSink(out)

	statsDone := make(chan struct{})
	go logThrottlerStats(throttler, statsDone)
	defer close(statsDone)

	go func() {
		source.
			Via(throttler).
			Via(flow.NewPassThrough()).
			To(sink)
	}()

	go produceBurst(in, 250)

	// Use a variable to prevent compiler optimization of CPU work
	var cpuWorkChecksum uint64

	for element := range sink.Out {
		fmt.Printf("consumer received %v\n", element)
		elementsProcessed.Add(1) // Track processed elements for memory pressure simulation

		// Perform CPU-intensive work that can't be optimized away
		// This ensures Windows GetProcessTimes can detect CPU usage
		// (Windows timer resolution is ~15.625ms, so we need at least 50-100ms of work)
		burnCPU(50*time.Millisecond, &cpuWorkChecksum)

		time.Sleep(25 * time.Millisecond)
	}

	// Print checksum to ensure CPU work wasn't optimized away
	fmt.Printf("CPU work checksum: %d\n", cpuWorkChecksum)

	fmt.Println("adaptive throttling pipeline completed")
}

// setupDemoThrottler creates and configures an adaptive throttler with demo settings
func setupDemoThrottler(elementsProcessed *atomic.Int64) *flow.AdaptiveThrottler {
	config := flow.DefaultAdaptiveThrottlerConfig()
	config.MinThroughput = 5
	config.MaxThroughput = 40
	config.SampleInterval = 200 * time.Millisecond
	config.BufferSize = 32
	config.AdaptationFactor = 0.5
	config.SmoothTransitions = true
	config.MaxMemoryPercent = 40.0
	config.MaxCPUPercent = 80.0

	config.MemoryReader = func() (float64, error) {
		elementCount := elementsProcessed.Load()

		// Memory pressure increases with processed elements:
		// - 0-50 elements: 5% memory
		// - 51-100 elements: 15% memory
		// - 101-150 elements: 30% memory
		// - 151+ elements: 50%+ memory (increases gradually)
		var memoryPercent float64
		switch {
		case elementCount <= 50:
			memoryPercent = 5.0 + float64(elementCount)*0.2 // 5% to 15%
		case elementCount <= 100:
			memoryPercent = 15.0 + float64(elementCount-50)*0.3 // 15% to 30%
		case elementCount <= 150:
			memoryPercent = 30.0 + float64(elementCount-100)*0.4 // 30% to 50%
		default:
			memoryPercent = 50.0 + float64(elementCount-150)*0.3 // 50%+ (increases more slowly)
			if memoryPercent > 95.0 {
				memoryPercent = 95.0
			}
		}

		return memoryPercent, nil
	}

	throttler, err := flow.NewAdaptiveThrottler(&config)
	if err != nil {
		panic(fmt.Sprintf("failed to create adaptive throttler: %v", err))
	}
	return throttler
}

func produceBurst(in chan<- any, total int) {
	defer close(in)

	for i := 0; i < total; i++ {
		in <- fmt.Sprintf("job-%02d", i)

		if (i+1)%10 == 0 {
			time.Sleep(180 * time.Millisecond)
			continue
		}

		time.Sleep(time.Duration(2+rand.Intn(5)) * time.Millisecond)
	}
}

// burnCPU performs CPU-intensive work for the specified duration
// The checksum parameter prevents the compiler from optimizing away the work
func burnCPU(duration time.Duration, checksum *uint64) {
	start := time.Now()
	for time.Since(start) < duration {
		for i := 0; i < 1000; i++ {
			*checksum += uint64(i * i)
		}
	}
}

func logThrottlerStats(at *flow.AdaptiveThrottler, done <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			stats := at.GetResourceStats()
			fmt.Printf("[stats] rate=%d eps memory=%.1f%% cpu=%.1f%% goroutines=%d\n",
				at.GetCurrentRate(), stats.MemoryUsedPercent, stats.CPUUsagePercent, stats.GoroutineCount)
		}
	}
}
