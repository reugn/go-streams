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

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, stringValues(e.([]interface{})))
	}
	fmt.Println(outputValues)

	assertEquals(t, 6, len(outputValues)) // [[a b c] [b c d] [c d e] [d e f g] [f g] [g]]

	assertEquals(t, []string{"a", "b", "c"}, outputValues[0])
	assertEquals(t, []string{"b", "c", "d"}, outputValues[1])
	// assertEquals(t, []string{"c", "d", "e"}, outputValues[2])
	// assertEquals(t, []string{"d", "e", "f", "g"}, outputValues[3])
	assertEquals(t, []string{"f", "g"}, outputValues[4])
	assertEquals(t, []string{"g"}, outputValues[5])
}

type element struct {
	value string
	ts    int64
}

func TestSlidingWindowWithExtractor(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	slidingWindow, err := flow.NewSlidingWindowWithTSExtractor(
		50*time.Millisecond,
		20*time.Millisecond,
		func(e element) int64 {
			return e.ts
		})

	if err != nil {
		t.Fatal(err)
	}

	sink := ext.NewChanSink(out)

	now := time.Now()
	inputValues := []element{
		{"a", now.Add(2 * time.Millisecond).UnixNano()},
		{"b", now.Add(17 * time.Millisecond).UnixNano()},
		{"c", now.Add(29 * time.Millisecond).UnixNano()},
		{"d", now.Add(35 * time.Millisecond).UnixNano()},
		{"e", now.Add(77 * time.Millisecond).UnixNano()},
		{"f", now.Add(93 * time.Millisecond).UnixNano()},
		{"g", now.Add(120 * time.Millisecond).UnixNano()},
	}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 250*time.Millisecond)

	go func() {
		source.
			Via(slidingWindow).
			To(sink)
	}()

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, stringValues(e.([]interface{})))
	}
	fmt.Println(outputValues)

	assertEquals(t, 6, len(outputValues)) // [[a b c d e f g] [c d e f g] [e f g] [e f g] [f g] [g]]

	assertEquals(t, []string{"a", "b", "c", "d", "e", "f", "g"}, outputValues[0])
	assertEquals(t, []string{"c", "d", "e", "f", "g"}, outputValues[1])
	assertEquals(t, []string{"e", "f", "g"}, outputValues[2])
	assertEquals(t, []string{"e", "f", "g"}, outputValues[3])
	assertEquals(t, []string{"f", "g"}, outputValues[4])
	assertEquals(t, []string{"g"}, outputValues[5])
}

func stringValues(elements []interface{}) []string {
	values := make([]string, len(elements))
	for i, e := range elements {
		switch v := e.(type) {
		case string:
			values[i] = v
		case element:
			values[i] = v.value
		}
	}
	return values
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
