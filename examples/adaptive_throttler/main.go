package main

import (
	"fmt"
	"strings"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func editMessage(msg string) string {
	return strings.ToUpper(msg)
}

func addTimestamp(msg string) string {
	return fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05"), msg)
}

func main() {
	// Configure first throttler - focuses on CPU limits
	throttler1Config := flow.DefaultAdaptiveThrottlerConfig()
	throttler1Config.MaxCPUPercent = 70.0
	throttler1Config.MaxMemoryPercent = 60.0
	throttler1Config.InitialRate = 15
	throttler1Config.MaxRate = 30
	throttler1Config.SampleInterval = 200 * time.Millisecond
	throttler1Config.CPUUsageMode = flow.CPUUsageModeMeasured // Use heuristic for better CPU visibility

	throttler1, err := flow.NewAdaptiveThrottler(throttler1Config)
	if err != nil {
		panic(fmt.Sprintf("failed to create throttler1: %v", err))
	}

	// Configure second throttler - focuses on memory limits
	throttler2Config := flow.DefaultAdaptiveThrottlerConfig()
	throttler2Config.MaxCPUPercent = 75.0
	throttler2Config.MaxMemoryPercent = 50.0
	throttler2Config.InitialRate = 10
	throttler2Config.MaxRate = 25
	throttler2Config.SampleInterval = 200 * time.Millisecond
	throttler2Config.CPUUsageMode = flow.CPUUsageModeMeasured // Use heuristic for better CPU visibility

	throttler2, err := flow.NewAdaptiveThrottler(throttler2Config)
	if err != nil {
		panic(fmt.Sprintf("failed to create throttler2: %v", err))
	}

	in := make(chan any)

	source := ext.NewChanSource(in)
	editMapFlow := flow.NewMap(editMessage, 1)
	timestampFlow := flow.NewMap(addTimestamp, 1)
	sink := ext.NewStdoutSink()

	// Pipeline: Source -> Throttler1 -> Edit -> Throttler2 -> Timestamp -> Sink
	go func() {
		source.
			Via(throttler1).
			Via(editMapFlow).
			Via(throttler2).
			Via(timestampFlow).
			To(sink)
	}()

	// Stats logging goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			stats := throttler1.GetResourceStats()
			fmt.Printf("[stats] T1-Rate: %.1f/s, T2-Rate: %.1f/s, CPU: %.1f%%, Mem: %.1f%%\n",
				throttler1.GetCurrentRate(),
				throttler2.GetCurrentRate(),
				stats.CPUUsagePercent,
				stats.MemoryUsedPercent)
		}
	}()

	go func() {
		defer close(in)

		for i := 1; i <= 100; i++ {
			message := fmt.Sprintf("message-%d", i)
			in <- message
			time.Sleep(30 * time.Millisecond)
		}
	}()

	sink.AwaitCompletion()
}
