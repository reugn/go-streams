package flow_test

import (
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

var addAsterisk = func(in string) []string {
	resultSlice := make([]string, 2)
	resultSlice[0] = in + "*"
	resultSlice[1] = in + "**"
	return resultSlice
}

var filterNotContainsA = func(in string) bool {
	return !strings.ContainsAny(in, "aA")
}

var reduceSum = func(a int, b int) int {
	return a + b
}

var retransmitStringSlice = func(in []string) []string {
	return in
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

	var outputValues []string
	for e := range sink.Out {
		outputValues = append(outputValues, e.(string))
	}

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

	var outputValues []string
	for e := range sink.Out {
		outputValues = append(outputValues, e.(string))
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

	var outputValues []string
	for e := range sink.Out {
		outputValues = append(outputValues, e.(string))
	}
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

	var outputValues []string
	for e := range sink.Out {
		outputValues = append(outputValues, e.(string))
	}
	sort.Strings(outputValues)

	expectedValues := []string{"B", "C"}
	assert.Equal(t, expectedValues, outputValues)
}

func TestReduceFlow(t *testing.T) {
	in := make(chan any, 5)
	out := make(chan any, 5)

	source := ext.NewChanSource(in)
	reduceFlow := flow.NewReduce(reduceSum)
	sink := ext.NewChanSink(out)

	inputValues := []int{1, 2, 3, 4, 5}
	ingestSlice(inputValues, in)
	close(in)

	source.
		Via(reduceFlow).
		Via(flow.NewPassThrough()).
		To(sink)

	var outputValues []int
	for e := range sink.Out {
		outputValues = append(outputValues, e.(int))
	}

	expectedValues := []int{1, 3, 6, 10, 15}
	assert.Equal(t, expectedValues, outputValues)
}

func TestFilterNonPositiveParallelism(t *testing.T) {
	assert.Panics(t, func() {
		flow.NewFilter(filterNotContainsA, 0)
	})
	assert.Panics(t, func() {
		flow.NewFilter(filterNotContainsA, -1)
	})
}

func TestFlatMapNonPositiveParallelism(t *testing.T) {
	assert.Panics(t, func() {
		flow.NewFlatMap(addAsterisk, 0)
	})
	assert.Panics(t, func() {
		flow.NewFlatMap(addAsterisk, -1)
	})
}

func TestMapNonPositiveParallelism(t *testing.T) {
	assert.Panics(t, func() {
		flow.NewMap(strings.ToUpper, 0)
	})
	assert.Panics(t, func() {
		flow.NewMap(strings.ToUpper, -1)
	})
}

// This test assures that the two sources s1=[1,2,3] and s2=[1,2,3] are zipped into a stream of [(1,1), (2,2), (3,3)]
func TestZipWith(t *testing.T) {
	in1 := make(chan interface{})
	in2 := make(chan interface{})
	out := make(chan interface{})

	source1 := ext.NewChanSource(in1)
	source2 := ext.NewChanSource(in2)

	// This combineFunc zips the two sources into an array:
	// If source1 emmitted 1 and also source2 emmited 1, thn combine func will output []int{1, 1}.
	combineFunc := func(elements *[]int) []int {
		return *elements
	}
	zipped := flow.ZipWith(combineFunc, source1, source2)

	sink := ext.NewChanSink(out)

	inputValues1 := []int{1, 2, 3}
	go ingestSlice(inputValues1, in1)
	go closeDeferred(in1, 3*time.Second)

	inputValues2 := []int{1, 2, 3}
	go ingestSlice(inputValues2, in2)
	go closeDeferred(in2, 3*time.Second)

	go func() {
		zipped.To(sink)
	}()

	var outputValues [][]int
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]int))
	}

	expectedValues := [][]int{{1, 1}, {2, 2}, {3, 3}}
	assertEquals(t, expectedValues, outputValues)
}
