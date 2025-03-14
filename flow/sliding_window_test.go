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
	in := make(chan any, 7)
	out := make(chan any, 7)

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

	source.
		Via(slidingWindow).
		To(sink)

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
	in := make(chan any, 10)
	out := make(chan any, 7)

	source := ext.NewChanSource(in)
	slidingWindow := flow.NewSlidingWindowWithOpts(
		50*time.Millisecond,
		20*time.Millisecond,
		flow.SlidingWindowOpts[element]{
			EventTimeExtractor: func(e element) time.Time {
				return time.UnixMilli(e.ts)
			},
		})
	sink := ext.NewChanSink(out)

	now := time.Now()
	inputValues := []element{
		{"c", now.Add(29 * time.Millisecond).UnixMilli()},
		{"a", now.Add(2 * time.Millisecond).UnixMilli()},
		{"b", now.Add(17 * time.Millisecond).UnixMilli()},
		{"d", now.Add(35 * time.Millisecond).UnixMilli()},
		{"f", now.Add(93 * time.Millisecond).UnixMilli()},
		{"e", now.Add(77 * time.Millisecond).UnixMilli()},
		{"g", now.Add(119 * time.Millisecond).UnixMilli()},
	}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 250*time.Millisecond)

	// send some out-of-order events
	go ingestDeferred(element{"h", now.Add(5 * time.Millisecond).UnixMilli()},
		in, 145*time.Millisecond)
	go ingestDeferred(element{"i", now.Add(3 * time.Millisecond).UnixMilli()},
		in, 145*time.Millisecond)

	source.
		Via(slidingWindow).
		To(sink)

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
	in := make(chan any, 10)
	out := make(chan any, 10)

	source := ext.NewChanSource(in)
	slidingWindow := flow.NewSlidingWindowWithOpts(
		50*time.Millisecond,
		20*time.Millisecond,
		flow.SlidingWindowOpts[*element]{
			EventTimeExtractor: func(e *element) time.Time {
				return time.UnixMilli(e.ts)
			},
			EmitPartialWindow: true,
		})
	sink := ext.NewChanSink(out)

	now := time.Now()
	inputValues := []*element{
		{"c", now.Add(29 * time.Millisecond).UnixMilli()},
		{"a", now.Add(2 * time.Millisecond).UnixMilli()},
		{"b", now.Add(17 * time.Millisecond).UnixMilli()},
		{"d", now.Add(35 * time.Millisecond).UnixMilli()},
		{"f", now.Add(93 * time.Millisecond).UnixMilli()},
		{"e", now.Add(77 * time.Millisecond).UnixMilli()},
		{"g", now.Add(119 * time.Millisecond).UnixMilli()},
	}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 250*time.Millisecond)

	// send some out-of-order events
	go ingestDeferred(&element{"h", now.Add(5 * time.Millisecond).UnixMilli()},
		in, 145*time.Millisecond)
	go ingestDeferred(&element{"i", now.Add(3 * time.Millisecond).UnixMilli()},
		in, 145*time.Millisecond)

	source.
		Via(slidingWindow).
		Via(flow.NewPassThrough()). // Via coverage
		To(sink)

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, elementValuesPtr(e.([]*element)))
	}
	fmt.Println(outputValues)

	assert.Equal(t, 8, len(outputValues)) // [[a b] [a b c d] [b c d] [d e] [e f] [e f g] [f g] [i h g]]

	assert.Equal(t, []string{"a", "b"}, outputValues[0])
	assert.Equal(t, []string{"a", "b", "c", "d"}, outputValues[1])
	assert.Equal(t, []string{"b", "c", "d"}, outputValues[2])
	assert.Equal(t, []string{"d", "e"}, outputValues[3])
	assert.Equal(t, []string{"e", "f"}, outputValues[4])
	assert.Equal(t, []string{"e", "f", "g"}, outputValues[5])
	assert.Equal(t, []string{"f", "g"}, outputValues[6])
	assert.Equal(t, []string{"i", "h", "g"}, outputValues[7])
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

func TestSlidingWindow_EarlyStreamClosure(t *testing.T) {
	in := make(chan any, 3)
	out := make(chan any, 3)

	source := ext.NewChanSource(in)
	slidingWindow := flow.NewSlidingWindow[element](50*time.Millisecond, 20*time.Millisecond)
	sink := ext.NewChanSink(out)

	now := time.Now()
	inputValues := []element{
		{"a", now.Add(2 * time.Millisecond).UnixMilli()},
		{"b", now.Add(17 * time.Millisecond).UnixMilli()},
	}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 10*time.Millisecond)

	source.
		Via(slidingWindow).
		To(sink)

	outputValues := readSlice[[]any](sink.Out)
	fmt.Println(outputValues)

	assert.Equal(t, 0, len(outputValues))
}
