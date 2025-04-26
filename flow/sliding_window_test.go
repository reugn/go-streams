package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

func TestSlidingWindow_SystemTime(t *testing.T) {
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
		close(in)
	}()

	source.
		Via(slidingWindow).
		To(sink)

	outputValues := readSlice[[]string](sink.Out)
	fmt.Println(outputValues)

	// The length of the output may vary when using processing time.
	// This is due to the imprecision in determining the lower boundary
	// of the first window.
	outputLen := len(outputValues)
	if outputLen < 4 || outputLen > 5 {
		t.Fatalf("Unexpected output size: %d", outputLen)
	}
}

type element struct {
	value string
	ts    int64
}

func TestSlidingWindow_WithExtractor(t *testing.T) {
	in := make(chan any, 10)
	out := make(chan any, 10)

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

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)
	inputValues := []element{
		{"a", now.Add(2 * time.Millisecond).UnixMilli()},
		{"b", now.Add(17 * time.Millisecond).UnixMilli()},
		{"c", now.Add(26 * time.Millisecond).UnixMilli()},
		{"d", now.Add(35 * time.Millisecond).UnixMilli()},
		{"f", now.Add(93 * time.Millisecond).UnixMilli()},
		{"e", now.Add(77 * time.Millisecond).UnixMilli()},
		{"g", now.Add(118 * time.Millisecond).UnixMilli()},
	}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 250*time.Millisecond)

	go ingestDeferred(element{"h", now.Add(140 * time.Millisecond).UnixMilli()},
		in, 140*time.Millisecond)

	// late event
	go ingestDeferred(element{"i", now.Add(80 * time.Millisecond).UnixMilli()},
		in, 135*time.Millisecond)

	source.
		Via(slidingWindow).
		To(sink)

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, elementValues(e.([]element)))
	}
	fmt.Println(outputValues)

	// [[a b c d] [c d] [e] [e f] [f g] [g h] [h]]
	assert.Equal(t, 7, len(outputValues))

	assert.Equal(t, []string{"a", "b", "c", "d"}, outputValues[0])
	assert.Equal(t, []string{"c", "d"}, outputValues[1])
	assert.Equal(t, []string{"e"}, outputValues[2])
	assert.Equal(t, []string{"e", "f"}, outputValues[3])
	assert.Equal(t, []string{"f", "g"}, outputValues[4])
	assert.Equal(t, []string{"g", "h"}, outputValues[5])
	assert.Equal(t, []string{"h"}, outputValues[6])
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
			AllowedLateness:   10 * time.Millisecond,
		})
	sink := ext.NewChanSink(out)

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.Local)
	inputValues := []*element{
		{"a", now.Add(2 * time.Millisecond).UnixMilli()},
		{"b", now.Add(17 * time.Millisecond).UnixMilli()},
		{"c", now.Add(26 * time.Millisecond).UnixMilli()},
		{"d", now.Add(35 * time.Millisecond).UnixMilli()},
		{"f", now.Add(93 * time.Millisecond).UnixMilli()},
		{"e", now.Add(77 * time.Millisecond).UnixMilli()},
		{"g", now.Add(118 * time.Millisecond).UnixMilli()},
	}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 250*time.Millisecond)

	go ingestDeferred(&element{"h", now.Add(135 * time.Millisecond).UnixMilli()},
		in, 135*time.Millisecond)

	// late event allowed
	go ingestDeferred(&element{"i", now.Add(118 * time.Millisecond).UnixMilli()},
		in, 125*time.Millisecond)
	// late event discarded
	go ingestDeferred(&element{"j", now.Add(10 * time.Millisecond).UnixMilli()},
		in, 125*time.Millisecond)

	source.
		Via(slidingWindow).
		Via(flow.NewPassThrough()). // Via coverage
		To(sink)

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, elementValuesPtr(e.([]*element)))
	}
	fmt.Println(outputValues)

	// [[a b c d] [c d] [e] [e f] [f g] [g i] [h]]
	assert.Equal(t, 7, len(outputValues))

	assert.Equal(t, []string{"a", "b", "c", "d"}, outputValues[0])
	assert.Equal(t, []string{"c", "d"}, outputValues[1])
	assert.Equal(t, []string{"e"}, outputValues[2])
	assert.Equal(t, []string{"e", "f"}, outputValues[3])
	assert.Equal(t, []string{"f", "g"}, outputValues[4])
	assert.Equal(t, []string{"g", "i"}, outputValues[5])
	assert.Equal(t, []string{"h"}, outputValues[6])
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
	assert.Panics(t, func() {
		flow.NewSlidingWindowWithOpts[string](
			10*time.Millisecond,
			5*time.Millisecond,
			flow.SlidingWindowOpts[string]{AllowedLateness: 10 * time.Millisecond},
		)
	})
}

func TestSlidingWindow(t *testing.T) {
	tests := []struct {
		name         string
		input        []element
		closeAfter   time.Duration
		outputLength int
	}{
		{
			name:         "emptyStream",
			input:        []element{},
			closeAfter:   time.Millisecond,
			outputLength: 0,
		},
		{
			name: "earlyStreamClosure",
			input: []element{
				{"a", time.Now().Add(2 * time.Millisecond).UnixMilli()},
			},
			closeAfter:   10 * time.Millisecond,
			outputLength: 0,
		},
		{
			name: "zeroTime",
			input: []element{
				{"a", time.Time{}.UnixMilli()},
			},
			closeAfter:   100 * time.Millisecond,
			outputLength: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make(chan any, 1)
			out := make(chan any, 1)

			source := ext.NewChanSource(in)
			slidingWindow := flow.NewSlidingWindowWithOpts[element](
				50*time.Millisecond,
				20*time.Millisecond,
				flow.SlidingWindowOpts[element]{
					EventTimeExtractor: func(e element) time.Time {
						return time.UnixMilli(e.ts)
					},
				})
			sink := ext.NewChanSink(out)

			go ingestSlice(tt.input, in)
			go closeDeferred(in, tt.closeAfter)

			source.
				Via(slidingWindow).
				To(sink)

			outputValues := readSlice[[]element](sink.Out)
			fmt.Println(outputValues)

			assert.Equal(t, tt.outputLength, len(outputValues))
		})
	}
}
