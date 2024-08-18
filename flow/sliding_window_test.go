package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

func TestSlidingWindow(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	slidingWindow := flow.NewSlidingWindow[string](50*time.Millisecond, 20*time.Millisecond)
	sink := ext.NewChanSink(out)
	assert.NotEqual(t, slidingWindow.Out(), nil)

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

	outputValues := readSlice[[]string](sink.Out)
	fmt.Println(outputValues)

	assert.Equal(t, 6, len(outputValues)) // [[a b c] [b c d] [c d e] [d e f g] [f g] [g]]

	assert.Equal(t, []string{"a", "b", "c"}, outputValues[0])
	assert.Equal(t, []string{"b", "c", "d"}, outputValues[1])
	// assert.Equal(t, []string{"c", "d", "e"}, outputValues[2])
	// assert.Equal(t, []string{"d", "e", "f", "g"}, outputValues[3])
	assert.Equal(t, []string{"f", "g"}, outputValues[4])
	assert.Equal(t, []string{"g"}, outputValues[5])
}

type element struct {
	value string
	ts    int64
}

func TestSlidingWindow_WithExtractor(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	slidingWindow := flow.NewSlidingWindowWithExtractor(
		50*time.Millisecond,
		20*time.Millisecond,
		func(e element) int64 {
			return e.ts
		})
	sink := ext.NewChanSink(out)

	now := time.Now()
	inputValues := []element{
		{"c", now.Add(29 * time.Millisecond).UnixNano()},
		{"a", now.Add(2 * time.Millisecond).UnixNano()},
		{"b", now.Add(17 * time.Millisecond).UnixNano()},
		{"d", now.Add(35 * time.Millisecond).UnixNano()},
		{"f", now.Add(93 * time.Millisecond).UnixNano()},
		{"e", now.Add(77 * time.Millisecond).UnixNano()},
		{"g", now.Add(120 * time.Millisecond).UnixNano()},
	}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 250*time.Millisecond)
	// send some out-of-order events
	go ingestDeferred(element{"h", now.Add(5 * time.Millisecond).UnixNano()},
		in, 145*time.Millisecond)
	go ingestDeferred(element{"i", now.Add(3 * time.Millisecond).UnixNano()},
		in, 145*time.Millisecond)

	go func() {
		source.
			Via(slidingWindow).
			To(sink)
	}()

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, elementValues(e.([]element)))
	}
	fmt.Println(outputValues)

	assert.Equal(t, 6, len(outputValues)) // [[a b c d] [c d] [e] [e f] [f g] [i h g]]

	assert.Equal(t, []string{"a", "b", "c", "d"}, outputValues[0])
	assert.Equal(t, []string{"c", "d"}, outputValues[1])
	assert.Equal(t, []string{"e"}, outputValues[2])
	assert.Equal(t, []string{"e", "f"}, outputValues[3])
	assert.Equal(t, []string{"f", "g"}, outputValues[4])
	assert.Equal(t, []string{"i", "h", "g"}, outputValues[5])
}

func elementValues(elements []element) []string {
	values := make([]string, len(elements))
	for i, e := range elements {
		values[i] = e.value
	}
	return values
}

func TestSlidingWindow_WithExtractorPtr(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	slidingWindow := flow.NewSlidingWindowWithExtractor(
		50*time.Millisecond,
		20*time.Millisecond,
		func(e *element) int64 {
			return e.ts
		})
	sink := ext.NewChanSink(out)

	now := time.Now()
	inputValues := []*element{
		{"c", now.Add(29 * time.Millisecond).UnixNano()},
		{"a", now.Add(2 * time.Millisecond).UnixNano()},
		{"b", now.Add(17 * time.Millisecond).UnixNano()},
		{"d", now.Add(35 * time.Millisecond).UnixNano()},
		{"f", now.Add(93 * time.Millisecond).UnixNano()},
		{"e", now.Add(77 * time.Millisecond).UnixNano()},
		{"g", now.Add(120 * time.Millisecond).UnixNano()},
	}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 250*time.Millisecond)
	// send some out-of-order events
	go ingestDeferred(&element{"h", now.Add(5 * time.Millisecond).UnixNano()},
		in, 145*time.Millisecond)
	go ingestDeferred(&element{"i", now.Add(3 * time.Millisecond).UnixNano()},
		in, 145*time.Millisecond)

	go func() {
		source.
			Via(slidingWindow).
			Via(flow.NewPassThrough()). // Via coverage
			To(sink)
	}()

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, elementValuesPtr(e.([]*element)))
	}
	fmt.Println(outputValues)

	assert.Equal(t, 6, len(outputValues)) // [[a b c d] [c d] [e] [e f] [f g] [i h g]]

	assert.Equal(t, []string{"a", "b", "c", "d"}, outputValues[0])
	assert.Equal(t, []string{"c", "d"}, outputValues[1])
	assert.Equal(t, []string{"e"}, outputValues[2])
	assert.Equal(t, []string{"e", "f"}, outputValues[3])
	assert.Equal(t, []string{"f", "g"}, outputValues[4])
	assert.Equal(t, []string{"i", "h", "g"}, outputValues[5])
}

func elementValuesPtr(elements []*element) []string {
	values := make([]string, len(elements))
	for i, e := range elements {
		values[i] = e.value
	}
	return values
}

func TestSlidingWindow_InvalidArguments(t *testing.T) {
	assert.Panics(t, func() {
		flow.NewSlidingWindow[string](10*time.Millisecond, 20*time.Millisecond)
	})
}
