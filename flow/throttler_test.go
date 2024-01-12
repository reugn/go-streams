package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

func TestThrottlerWithBackpressure(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	interval := 10 * time.Millisecond
	source := ext.NewChanSource(in)
	throttler := flow.NewThrottler(2, interval, 2, flow.Backpressure)
	sink := ext.NewChanSink(out)

	go writeValues(in)

	go func() {
		source.
			Via(throttler).
			To(sink)
	}()

	outputValues := readValues(interval/2, out)
	assert.Equal(t, []any{"a", "b"}, outputValues)
	fmt.Println(outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	assert.Equal(t, []any{"c", "d"}, outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	assert.Equal(t, []any{"e", "f"}, outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	assert.Equal(t, []any{"g"}, outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	var empty []any
	assert.Equal(t, empty, outputValues)
}

func TestThrottlerWithDiscard(t *testing.T) {
	in := make(chan any, 7)
	out := make(chan any, 7)

	interval := 20 * time.Millisecond
	source := ext.NewChanSource(in)
	throttler := flow.NewThrottler(2, interval, 1, flow.Discard)
	sink := ext.NewChanSink(out)

	go writeValues(in)

	go func() {
		source.
			Via(throttler).
			To(sink)
	}()

	outputValues := readValues(interval/2, out)
	assert.Equal(t, []any{"a", "b"}, outputValues)
	fmt.Println(outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	var empty []any
	assert.Equal(t, empty, outputValues)
}

func TestThrottlerNonPositiveElements(t *testing.T) {
	assert.Panics(t, func() {
		flow.NewThrottler(0, time.Second, 1, flow.Discard)
	})
	assert.Panics(t, func() {
		flow.NewThrottler(-1, time.Second, 1, flow.Discard)
	})
}

func TestThrottlerNonPositiveBufferSize(t *testing.T) {
	assert.Panics(t, func() {
		flow.NewThrottler(1, time.Second, 0, flow.Backpressure)
	})
	assert.Panics(t, func() {
		flow.NewThrottler(1, time.Second, -1, flow.Backpressure)
	})
}

func writeValues(in chan any) {
	inputValues := []string{"a", "b", "c", "d", "e", "f", "g"}
	ingestSlice(inputValues, in)
	close(in)
	fmt.Println("Closed input channel")
}

func readValues(timeout time.Duration, out <-chan any) []any {
	var outputValues []any
	timer := time.NewTimer(timeout)
	for {
		select {
		case e := <-out:
			if e != nil {
				outputValues = append(outputValues, e)
			} else {
				fmt.Println("Got nil in output")
				timer.Stop()
				return outputValues
			}
		case <-timer.C:
			return outputValues
		}
	}
}
