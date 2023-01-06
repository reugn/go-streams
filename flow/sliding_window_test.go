package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func TestSlidingWindow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	slidingWindow, err := flow.NewSlidingWindow(50*time.Millisecond, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	sink := ext.NewChanSink(out)

	go func() {
		inputValues := []string{"a", "b", "c", "d", "e", "f", "g"}
		for _, v := range inputValues {
			ingestDeferred(v, in, 15*time.Millisecond)
		}
		closeDeferred(in, 250*time.Millisecond)
	}()

	go func() {
		source.
			Via(slidingWindow).
			To(sink)
	}()

	var outputValues [][]interface{}
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]interface{}))
	}
	fmt.Println(outputValues)

	assertEquals(t, 6, len(outputValues)) // [[a b c] [b c d] [c d e] [d e f g] [f g] [g]]

	assertEquals(t, []interface{}{"a", "b", "c"}, outputValues[0])
	assertEquals(t, []interface{}{"b", "c", "d"}, outputValues[1])
	assertEquals(t, []interface{}{"c", "d", "e"}, outputValues[2])
	assertEquals(t, []interface{}{"d", "e", "f", "g"}, outputValues[3])
	assertEquals(t, []interface{}{"f", "g"}, outputValues[4])
	assertEquals(t, []interface{}{"g"}, outputValues[5])
}

func TestSlidingWindowInvalidParameters(t *testing.T) {
	slidingWindow, err := flow.NewSlidingWindow(10*time.Millisecond, 20*time.Millisecond)
	if slidingWindow != nil {
		t.Fatal("slidingWindow should be nil")
	}
	if err == nil {
		t.Fatal("err should not be nil")
	}
}
