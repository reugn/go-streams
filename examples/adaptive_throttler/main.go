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

func main() {
	throttlerConfig := flow.DefaultAdaptiveThrottlerConfig()
	// To setup a custom throttler, you can modify the throttlerConfig struct with your desired values.
	// For all available options, see the flow.AdaptiveThrottlerConfig struct.
	// Example:
	// throttlerConfig.CPUUsageMode = flow.CPUUsageModeHeuristic
	// throttlerConfig.MaxMemoryPercent = 80.0
	// throttlerConfig.MaxCPUPercent = 70.0
	// throttlerConfig.MinThroughput = 10
	// throttlerConfig.MaxThroughput = 100
	// throttlerConfig.SampleInterval = 50 * time.Millisecond
	// throttlerConfig.BufferSize = 10
	// throttlerConfig.AdaptationFactor = 0.5
	// throttlerConfig.SmoothTransitions = false
	throttler := flow.NewAdaptiveThrottler(throttlerConfig)
	defer throttler.Close()

	in := make(chan any)

	source := ext.NewChanSource(in)
	editMapFlow := flow.NewMap(editMessage, 1)
	sink := ext.NewStdoutSink()

	go func() {
		source.Via(throttler).Via(editMapFlow).To(sink)
	}()

	go func() {
		defer close(in)

		for i := 1; i <= 50; i++ {
			message := fmt.Sprintf("message-%d", i)
			in <- message
			time.Sleep(50 * time.Millisecond)
		}
	}()

	sink.AwaitCompletion()
}
