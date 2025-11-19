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
	// To setup a custom throttler, you can modify the throttlerConfig struct with your desired values.
	// For all available options, see the flow.AdaptiveThrottlerConfig struct.
	throttlerConfig := flow.DefaultAdaptiveThrottlerConfig()
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
