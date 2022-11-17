package flow_test

import (
	"container/heap"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/util"
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
		fmt.Printf("ingest: %v", e)
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
	throttler := flow.NewThrottler(10, time.Second, 50, flow.Backpressure)
	slidingWindow := flow.NewSlidingWindow(2*time.Second, 2*time.Second)
	tumblingWindow := flow.NewTumblingWindow(time.Second)
	filterNotContainsA := flow.NewFilter(filterNotContainsA, 1)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 3*time.Second)

	go func() {
		source.
			Via(toUpperMapFlow).
			Via(appendAsteriskFlatMapFlow).
			Via(tumblingWindow).
			Via(flow.Flatten(1)).
			Via(slidingWindow).
			Via(throttler).
			Via(flow.Flatten(1)).
			Via(filterNotContainsA).
			To(sink)
	}()

	var outputValues []string
	for e := range sink.Out {
		outputValues = append(outputValues, e.(string))
	}

	expectedValues := []string{"B*", "B**", "C*", "C**"}
	assertEquals(t, expectedValues, outputValues)
}

func TestFanOutFlow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	filterNotContainsA := flow.NewFilter(filterNotContainsA, 1)
	toUpperMapFlow := flow.NewMap(strings.ToUpper, 1)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 100*time.Millisecond)

	go func() {
		fanOut := flow.FanOut(source.Via(filterNotContainsA).Via(toUpperMapFlow), 2)
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
	filterNotContainsA := flow.NewFilter(filterNotContainsA, 1)
	toUpperMapFlow := flow.NewMap(strings.ToUpper, 1)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 100*time.Millisecond)

	go func() {
		roundRobin := flow.RoundRobin(source.Via(filterNotContainsA).Via(toUpperMapFlow), 2)
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

func TestSessionWindow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	sessionWindow := flow.NewSessionWindow(200 * time.Millisecond)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go ingestDeferred("d", in, 300*time.Millisecond)
	go ingestDeferred("e", in, 700*time.Millisecond)
	go closeDeferred(in, time.Second)

	go func() {
		source.
			Via(sessionWindow).
			To(sink)
	}()

	var outputValues [][]interface{}
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]interface{}))
	}

	assertEquals(t, 3, len(outputValues))
	assertEquals(t, 3, len(outputValues[0]))
	assertEquals(t, 1, len(outputValues[1]))
	assertEquals(t, 1, len(outputValues[2]))
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

func TestQueue(t *testing.T) {
	queue := &flow.PriorityQueue{}
	heap.Push(queue, flow.NewItem(1, util.NowNano(), 0))
	heap.Push(queue, flow.NewItem(2, 1234, 0))
	heap.Push(queue, flow.NewItem(3, util.NowNano(), 0))
	queue.Swap(0, 1)
	head := queue.Head()
	queue.Update(head, util.NowNano())
	first := heap.Pop(queue).(*flow.Item)

	assertEquals(t, 2, first.Msg.(int))
}

func assertEquals[T any](t *testing.T, expected T, actual T) {
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("%v != %v", expected, actual)
	}
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
