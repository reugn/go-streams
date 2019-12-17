package flow_test

import (
	"container/heap"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/reugn/go-streams"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

var toUpper = func(in interface{}) interface{} {
	msg := in.(string)
	return strings.ToUpper(msg)
}

var appendAsterix = func(in interface{}) []interface{} {
	arr := in.([]interface{})
	rt := make([]interface{}, len(arr))
	for i, item := range arr {
		msg := item.(string)
		rt[i] = msg + "*"
	}
	return rt
}

var flatten = func(in interface{}) []interface{} {
	return in.([]interface{})
}

var filterA = func(in interface{}) bool {
	msg := in.(string)
	return msg != "a"
}

func ingest(source []string, in chan interface{}) {
	for _, e := range source {
		in <- e
	}
}

func deferClose(in chan interface{}, d time.Duration) {
	time.Sleep(d)
	close(in)
}

func TestFlow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	flow1 := flow.NewMap(toUpper, 1)
	flow2 := flow.NewFlatMap(appendAsterix, 1)
	flow3 := flow.NewFlatMap(flatten, 1)
	throttler := flow.NewThrottler(10, time.Second*1, 50, flow.Backpressure)
	slidingWindow := flow.NewSlidingWindow(time.Second*2, time.Second*2)
	tumblingWindow := flow.NewTumblingWindow(time.Second * 1)
	sink := ext.NewChanSink(out)

	var _input = []string{"a", "b", "c"}
	var _expectedOutput = []string{"A*", "B*", "C*"}

	go ingest(_input, in)
	go deferClose(in, time.Second*3)
	go func() {
		source.Via(flow1).
			Via(tumblingWindow).
			Via(flow3).
			Via(slidingWindow).
			Via(flow2).
			Via(throttler).
			To(sink)
	}()
	var _output []string
	for e := range sink.Out {
		_output = append(_output, e.(string))
	}
	assertEqual(t, _expectedOutput, _output)
}

func TestFlowUtil(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	flow1 := flow.NewMap(toUpper, 1)
	filter := flow.NewFilter(filterA, 1)
	sink := ext.NewChanSink(out)

	var _input = []string{"a", "b", "c"}
	var _expectedOutput = []string{"B", "B", "C", "C"}

	go ingest(_input, in)
	go deferClose(in, time.Second*1)
	go func() {
		fanOut := flow.FanOut(source.Via(filter).Via(flow1), 2)
		flow.Merge(fanOut...).To(sink)
	}()
	var _output []string
	for e := range sink.Out {
		_output = append(_output, e.(string))
	}
	sort.Strings(_output)
	assertEqual(t, _expectedOutput, _output)
}

func TestQueue(t *testing.T) {
	queue := &flow.PriorityQueue{}
	heap.Push(queue, flow.NewItem(1, streams.NowNano(), 0))
	heap.Push(queue, flow.NewItem(2, 1234, 0))
	heap.Push(queue, flow.NewItem(3, streams.NowNano(), 0))
	queue.Swap(0, 1)
	head := queue.Head()
	queue.Update(head, streams.NowNano())
	first := heap.Pop(queue).(*flow.Item)
	assertEqual(t, first.Msg.(int), 2)
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("%s != %s", a, b)
	}
}
