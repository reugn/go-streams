package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

// Demo of adaptive throttling with CPU-intensive work.
// T1 throttles on CPU > 0.01%, T2 throttles on Memory > 50%.

func editMessage(msg string) string {
	return strings.ToUpper(msg)
}

func addTimestamp(msg string) string {
	return fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
}

// cpuIntensiveWork simulates processing load that affects CPU usage
func cpuIntensiveWork(msg string) string {
	// Simulate CPU-intensive work (hashing-like computation)
	var checksum uint64
	for i := 0; i < 200000; i++ { // Increased from 50000 to 200000 for more CPU load
		checksum += uint64(len(msg)) * uint64(i) //nolint:gosec
	}
	return msg
}

func main() {
	var messagesProcessed atomic.Int64

	// Configure first throttler - CPU-focused with higher initial rate
	throttler1Config := flow.DefaultAdaptiveThrottlerConfig()
	throttler1Config.MaxCPUPercent = 0.01    // Throttle when CPU > 0.01% (extremely low threshold)
	throttler1Config.MaxMemoryPercent = 80.0 // Less strict memory limit
	throttler1Config.InitialRate = 50        // Start at 50/sec
	throttler1Config.MaxRate = 100           // Can go up to 100/sec
	throttler1Config.MinRate = 5             // Minimum 5/sec
	throttler1Config.SampleInterval = 200 * time.Millisecond
	throttler1Config.BackoffFactor = 0.6  // Reduce to 60% when constrained
	throttler1Config.RecoveryFactor = 1.4 // Increase by 40% during recovery

	throttler1, err := flow.NewAdaptiveThrottler(throttler1Config)
	if err != nil {
		panic(fmt.Sprintf("failed to create throttler1: %v", err))
	}

	// Configure second throttler - Memory-focused with memory simulation
	throttler2Config := flow.DefaultAdaptiveThrottlerConfig()
	throttler2Config.MaxCPUPercent = 80.0    // Less strict CPU limit
	throttler2Config.MaxMemoryPercent = 40.0 // Throttle when memory > 40%
	throttler2Config.InitialRate = 30        // Start at 30/sec
	throttler2Config.MaxRate = 80            // Can go up to 80/sec
	throttler2Config.MinRate = 3             // Minimum 3/sec
	throttler2Config.SampleInterval = 200 * time.Millisecond
	throttler2Config.BackoffFactor = 0.5  // Reduce to 50% when constrained
	throttler2Config.RecoveryFactor = 1.3 // Increase by 30% during recovery

	// Use system memory but with lower threshold to demonstrate throttling
	throttler2Config.MaxMemoryPercent = 50.0 // Lower threshold than T1

	throttler2, err := flow.NewAdaptiveThrottler(throttler2Config)
	if err != nil {
		panic(fmt.Sprintf("failed to create throttler2: %v", err))
	}

	in := make(chan any)

	source := ext.NewChanSource(in)
	editMapFlow := flow.NewMap(editMessage, 1)
	cpuWorkFlow := flow.NewMap(cpuIntensiveWork, 1) // Add CPU-intensive work
	timestampFlow := flow.NewMap(addTimestamp, 1)
	sink := ext.NewStdoutSink()

	// Pipeline: Source -> Throttler1 (CPU) -> CPU Work -> Throttler2 (Memory) -> Edit -> Timestamp -> Sink
	go func() {
		source.
			Via(throttler1).    // First throttler monitors CPU
			Via(cpuWorkFlow).   // CPU-intensive processing
			Via(throttler2).    // Second throttler monitors memory
			Via(editMapFlow).   // Simple transformation
			Via(timestampFlow). // Add timestamp
			To(sink)
	}()

	// Enhanced stats logging showing throttling behavior
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			stats1 := throttler1.GetResourceStats()
			stats2 := throttler2.GetResourceStats()

			// Show current rates and resource usage
			fmt.Printf("[stats] T1-CPU: %.1f/s (CPU:%.1f%%), T2-Mem: %.1f/s (Mem:%.1f%%), Total: %d msgs\n",
				throttler1.GetCurrentRate(),
				stats1.CPUUsagePercent,
				throttler2.GetCurrentRate(),
				stats2.MemoryUsedPercent,
				messagesProcessed.Load())

			// Show throttling status
			throttleReason := ""
			if stats1.CPUUsagePercent > throttler1Config.MaxCPUPercent {
				throttleReason += "T1:CPU-high "
			}
			if stats2.MemoryUsedPercent > throttler2Config.MaxMemoryPercent {
				throttleReason += "T2:Mem-high "
			}
			if throttleReason == "" {
				throttleReason = "No throttling"
			}
			fmt.Printf("[throttle] %s\n", throttleReason)
		}
	}()

	// Producer with bursty traffic to test throttling
	go func() {
		defer close(in)

		for i := 1; i <= 100; i++ {
			message := fmt.Sprintf("MESSAGE-%d", i)
			in <- message
			messagesProcessed.Add(1)

			// Variable delay to create bursts (some fast, some slow)
			var delay time.Duration
			if i%20 == 0 {
				delay = 100 * time.Millisecond // Burst pause every 20 messages
			} else {
				delay = time.Duration(5+rand.Intn(15)) * time.Millisecond //nolint:gosec // 5-20ms between messages
			}
			time.Sleep(delay)
		}
	}()

	sink.AwaitCompletion()

	fmt.Printf("Demo completed! Processed %d messages\n", messagesProcessed.Load())
}
