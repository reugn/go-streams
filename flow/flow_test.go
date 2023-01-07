package flow_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
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

func ingestSlice[T any](source []T, in chan interface{}) {
	for _, e := range source {
		in <- e
	}
}

func ingestDeferred[T any](item T, in chan interface{}, wait time.Duration) {
	time.Sleep(wait)
	in <- item
}

func closeDeferred[T any](in chan T, wait time.Duration) {
	time.Sleep(wait)
	close(in)
}

func TestComplexFlow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	toUpperMapFlow := flow.NewMap(strings.ToUpper, 1)
	appendAsteriskFlatMapFlow := flow.NewFlatMap(addAsterisk, 1)
	throttler := flow.NewThrottler(10, 200*time.Millisecond, 50, flow.Backpressure)
	tumblingWindow := flow.NewTumblingWindow(200 * time.Millisecond)
	filterFlow := flow.NewFilter(filterNotContainsA, 1)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, time.Second)

	go func() {
		source.
			Via(toUpperMapFlow).
			Via(appendAsteriskFlatMapFlow).
			Via(tumblingWindow).
			Via(flow.Flatten(1)).
			Via(throttler).
			Via(filterFlow).
			To(sink)
	}()

	var outputValues []string
	for e := range sink.Out {
		outputValues = append(outputValues, e.(string))
	}

	expectedValues := []string{"B*", "B**", "C*", "C**"}
	assertEquals(t, expectedValues, outputValues)
}

func TestSplitFlow(t *testing.T) {
	in := make(chan interface{}, 3)
	out := make(chan interface{}, 3)

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
	assertEquals(t, expectedValues, outputValues)
}

func TestFanOutFlow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

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
	assertEquals(t, expectedValues, outputValues)
}

func TestRoundRobinFlow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

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
	assertEquals(t, expectedValues, outputValues)
}

func TestReduceFlow(t *testing.T) {
	in := make(chan interface{}, 5)
	out := make(chan interface{}, 5)

	source := ext.NewChanSource(in)
	reduceFlow := flow.NewReduce(reduceSum)
	sink := ext.NewChanSink(out)

	inputValues := []int{1, 2, 3, 4, 5}
	ingestSlice(inputValues, in)
	close(in)

	source.
		Via(reduceFlow).
		To(sink)

	var outputValues []int
	for e := range sink.Out {
		outputValues = append(outputValues, e.(int))
	}

	expectedValues := []int{1, 3, 6, 10, 15}
	assertEquals(t, expectedValues, outputValues)
}

func assertEquals[T any](t *testing.T, expected T, actual T) {
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("%v != %v", expected, actual)
	}
}
