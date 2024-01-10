package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func TestThrottlerWithBackpressure(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

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
	assertEquals(t, []interface{}{"a", "b"}, outputValues)
	fmt.Println(outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	assertEquals(t, []interface{}{"c", "d"}, outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	assertEquals(t, []interface{}{"e", "f"}, outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	assertEquals(t, []interface{}{"g"}, outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	var empty []interface{}
	assertEquals(t, empty, outputValues)
}

func TestThrottlerWithDiscard(t *testing.T) {
	in := make(chan interface{}, 7)
	out := make(chan interface{}, 7)

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
	assertEquals(t, []interface{}{"a", "b"}, outputValues)
	fmt.Println(outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)

	outputValues = readValues(interval, out)
	fmt.Println(outputValues)
	var empty []interface{}
	assertEquals(t, empty, outputValues)
}

func writeValues(in chan interface{}) {
	inputValues := []string{"a", "b", "c", "d", "e", "f", "g"}
	ingestSlice(inputValues, in)
	close(in)
	fmt.Println("Closed input channel")
}

func readValues(timeout time.Duration, out <-chan interface{}) []interface{} {
	var outputValues []interface{}
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
