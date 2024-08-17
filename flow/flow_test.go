package flow_test

import (
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/reugn/go-streams"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

func ptr[T any](value T) *T {
	return &value
}

func ptrSlice[T any](slice []T) []*T {
	result := make([]*T, len(slice))
	for i, e := range slice {
		result[i] = ptr(e)
	}
	return result
}

func ptrInnerSlice[T any](slice [][]T) [][]*T {
	outer := make([][]*T, len(slice))
	for i, s := range slice {
		inner := make([]*T, len(s))
		for j, e := range s {
			inner[j] = ptr(e)
		}
		outer[i] = inner
	}
	return outer
}

var addAsterisk = func(in string) []string {
	resultSlice := make([]string, 2)
	resultSlice[0] = in + "*"
	resultSlice[1] = in + "**"
	return resultSlice
}

var filterNotContainsA = func(in string) bool {
	return !strings.ContainsAny(in, "aA")
}

var mtx sync.Mutex

func ingestSlice[T any](source []T, in chan any) {
	mtx.Lock()
	defer mtx.Unlock()
	for _, e := range source {
		in <- e
	}
}

func ingestDeferred[T any](item T, in chan any, wait time.Duration) {
	time.Sleep(wait)
	mtx.Lock()
	defer mtx.Unlock()
	in <- item
}

func closeDeferred[T any](in chan T, wait time.Duration) {
	time.Sleep(wait)
	mtx.Lock()
	defer mtx.Unlock()
	close(in)
}

func readSlice[T any](ch <-chan any) []T {
	var result []T
	for e := range ch {
		result = append(result, e.(T))
	}
	return result
}

func readSlicePtr[T any](ch <-chan any) []*T {
	var result []*T
	for e := range ch {
		result = append(result, e.(*T))
	}
	return result
}

func TestComplexFlow(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	toUpperMapFlow := flow.NewMap(strings.ToUpper, 1)
	appendAsteriskFlatMapFlow := flow.NewFlatMap(addAsterisk, 1)
	throttler := flow.NewThrottler(10, 200*time.Millisecond, 50, flow.Backpressure)
	tumblingWindow := flow.NewTumblingWindow[string](200 * time.Millisecond)
	filterFlow := flow.NewFilter(filterNotContainsA, 1)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, time.Second)

	go func() {
		source.
			Via(toUpperMapFlow).
			Via(flow.NewPassThrough()).
			Via(appendAsteriskFlatMapFlow).
			Via(tumblingWindow).
			Via(flow.Flatten[string](1)).
			Via(throttler).
			Via(filterFlow).
			To(sink)
	}()

	outputValues := readSlice[string](sink.Out)
	expectedValues := []string{"B*", "B**", "C*", "C**"}

	assert.Equal(t, expectedValues, outputValues)
}

func TestSplitFlow(t *testing.T) {
	in := make(chan any, 3)
	out := make(chan any, 3)

	source := ext.NewChanSource(in)
	toUpperMapFlow := flow.NewMap(strings.ToUpper, 1)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	ingestSlice(inputValues, in)
	close(in)

	split := flow.Split(
		source.
			Via(toUpperMapFlow), filterNotContainsA)

	flow.Merge(split[0], split[1]).
		To(sink)

	outputValues := readSlice[string](sink.Out)
	sort.Strings(outputValues)
	expectedValues := []string{"A", "B", "C"}

	assert.Equal(t, expectedValues, outputValues)
}

func TestSplitFlow_Ptr(t *testing.T) {
	in := make(chan any, 3)
	out := make(chan any, 3)

	source := ext.NewChanSource(in)
	toUpperMapFlow := flow.NewMap(func(s *string) *string {
		upper := strings.ToUpper(*s)
		return &upper
	}, 1)
	sink := ext.NewChanSink(out)

	inputValues := ptrSlice([]string{"a", "b", "c"})
	ingestSlice(inputValues, in)
	close(in)

	split := flow.Split(
		source.Via(toUpperMapFlow),
		func(in *string) bool {
			return !strings.ContainsAny(*in, "aA")
		})

	flow.Merge(split[0], split[1]).
		To(sink)

	var outputValues []string
	for e := range sink.Out {
		v := e.(*string)
		outputValues = append(outputValues, *v)
	}
	sort.Strings(outputValues)
	expectedValues := []string{"A", "B", "C"}

	assert.Equal(t, expectedValues, outputValues)
}

func TestFanOutFlow(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	filterFlow := flow.NewFilter(filterNotContainsA, 1)
	toUpperMapFlow := flow.NewMap(strings.ToUpper, 1)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 100*time.Millisecond)

	go func() {
		fanOut := flow.FanOut(
			source.
				Via(filterFlow).
				Via(toUpperMapFlow), 2)
		flow.
			Merge(fanOut...).
			To(sink)
	}()

	outputValues := readSlice[string](sink.Out)
	sort.Strings(outputValues)
	expectedValues := []string{"B", "B", "C", "C"}

	assert.Equal(t, expectedValues, outputValues)
}

func TestRoundRobinFlow(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	filterFlow := flow.NewFilter(filterNotContainsA, 1)
	toUpperMapFlow := flow.NewMap(strings.ToUpper, 1)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 100*time.Millisecond)

	go func() {
		roundRobin := flow.RoundRobin(
			source.
				Via(filterFlow).
				Via(toUpperMapFlow), 2)
		flow.
			Merge(roundRobin...).
			To(sink)
	}()

	outputValues := readSlice[string](sink.Out)
	sort.Strings(outputValues)
	expectedValues := []string{"B", "C"}

	assert.Equal(t, expectedValues, outputValues)
}

func TestFlatten(t *testing.T) {
	tests := []struct {
		name        string
		flattenFlow streams.Flow
		ptr         bool
	}{
		{
			name:        "values",
			flattenFlow: flow.Flatten[int](1),
			ptr:         false,
		},
		{
			name:        "pointers",
			flattenFlow: flow.Flatten[*int](1),
			ptr:         true,
		},
	}
	input := [][]int{{1, 2, 3}, {4, 5}}
	expected := []int{1, 2, 3, 4, 5}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make(chan any, 5)
			out := make(chan any, 5)

			source := ext.NewChanSource(in)
			sink := ext.NewChanSink(out)

			if tt.ptr {
				ingestSlice(ptrInnerSlice(input), in)
			} else {
				ingestSlice(input, in)
			}
			close(in)

			source.
				Via(tt.flattenFlow).
				To(sink)

			if tt.ptr {
				output := readSlicePtr[int](out)
				assert.Equal(t, ptrSlice(expected), output)
			} else {
				output := readSlice[int](out)
				assert.Equal(t, expected, output)
			}
		})
	}
}
