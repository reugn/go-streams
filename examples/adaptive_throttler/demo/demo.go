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
// 3. Simulates memory pressure that increases then decreases (creating throttling-recovery cycle)
// 4. The adaptive throttler adjusts throughput based on CPU/memory usage
// 5. Shows throttling down to ~1/sec during high memory, then recovery back to 40/sec
// 6. Stats are logged every 500ms showing rate adaptation
func main() {
	var elementsProcessed atomic.Int64

	// Set up demo configuration with memory simulation
	throttler := setupDemoThrottler(&elementsProcessed)

	in := make(chan any)
	out := make(chan any) // Unbuffered channel to prevent apparent bursts

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

	var cpuWorkChecksum uint64

	// Process the output
	elementsReceived := 0
	for element := range sink.Out {
		fmt.Printf("consumer received %v\n", element)
		elementsProcessed.Add(1)
		elementsReceived++

		// Perform CPU-intensive work
		burnCPU(50*time.Millisecond, &cpuWorkChecksum)

		time.Sleep(25 * time.Millisecond)
	}

	fmt.Printf("CPU work checksum: %d\n", cpuWorkChecksum)
	fmt.Printf("Total elements produced: 250, Total elements received: %d\n", elementsReceived)
	if elementsReceived == 250 {
		fmt.Println("✅ SUCCESS: All elements processed without dropping!")
	} else {
		fmt.Printf("❌ FAILURE: %d elements were dropped!\n", 250-elementsReceived)
	}
	fmt.Println("adaptive throttling pipeline completed")
}

// setupDemoThrottler creates and configures an adaptive throttler with demo settings
func setupDemoThrottler(elementsProcessed *atomic.Int64) *flow.AdaptiveThrottler {
	config := flow.DefaultAdaptiveThrottlerConfig()

	config.MinRate = 1
	config.MaxRate = 20
	config.InitialRate = 20
	config.SampleInterval = 200 * time.Millisecond

	config.BackoffFactor = 0.5
	config.RecoveryFactor = 1.5

	config.MaxMemoryPercent = 35.0
	config.RecoveryMemoryThreshold = 30.0

	// Memory Reader Simulation - Creates a cycle: low -> high -> low memory usage
	config.MemoryReader = func() (float64, error) {
		elementCount := elementsProcessed.Load()

		var memoryPercent float64
		switch {
		case elementCount <= 80: // Phase 1: Low memory, allow high throughput
			memoryPercent = 5.0 + float64(elementCount)*0.1 // 5% to 13%
		case elementCount <= 120: // Phase 2: Increasing memory pressure, cause throttling
			memoryPercent = 15.0 + float64(elementCount-80)*0.6 // 15% to 43%
		case elementCount <= 160: // Phase 3: High memory, keep throttled down to ~1/sec
			memoryPercent = 30.0 + float64(elementCount-120)*0.3 // 30% to 42%
		default: // Phase 4: Memory decreases, allow recovery back to 40/sec
			memoryPercent = 25.0 - float64(elementCount-160)*1.5 // 25% down to ~5%
			if memoryPercent < 5.0 {
				memoryPercent = 5.0
			}
		}
		return memoryPercent, nil
	}

	throttler, err := flow.NewAdaptiveThrottler(config)
	if err != nil {
		panic(fmt.Sprintf("failed to create adaptive throttler: %v", err))
	}
	return throttler
}

func produceBurst(in chan<- any, total int) {
	defer close(in)

	for i := range total {
		in <- fmt.Sprintf("job-%02d", i)

		if (i+1)%10 == 0 {
			time.Sleep(180 * time.Millisecond)
			continue
		}
		time.Sleep(time.Duration(2+rand.Intn(5)) * time.Millisecond)
	}
}

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
			fmt.Printf("[stats] Rate: %.1f/sec, CPU: %.1f%%, Memory: %.1f%%\n",
				at.GetCurrentRate(), stats.CPUUsagePercent, stats.MemoryUsedPercent)
		}
	}
}
