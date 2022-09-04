package flow_test

import (
	"container/heap"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/util"
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

func ingestDeferred(item string, in chan interface{}, wait time.Duration) {
	time.Sleep(wait)
	in <- item
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
	throttler := flow.NewThrottler(10, time.Second, 50, flow.Backpressure)
	slidingWindow := flow.NewSlidingWindow(2*time.Second, 2*time.Second)
	tumblingWindow := flow.NewTumblingWindow(time.Second)
	sink := ext.NewChanSink(out)

	var _input = []string{"a", "b", "c"}
	var _expectedOutput = []string{"A*", "B*", "C*"}

	go ingest(_input, in)
	go deferClose(in, 3*time.Second)
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
	t.Run("FanOut", func(t *testing.T) {
		in := make(chan interface{})
		out := make(chan interface{})

		source := ext.NewChanSource(in)
		flow1 := flow.NewMap(toUpper, 1)
		filter := flow.NewFilter(filterA, 1)
		sink := ext.NewChanSink(out)

		var _input = []string{"a", "b", "c"}
		var _expectedOutput = []string{"B", "B", "C", "C"}

		go ingest(_input, in)
		go deferClose(in, time.Second)
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
	})
	t.Run("RoundRobin", func(t *testing.T) {
		in := make(chan interface{})
		out := make(chan interface{})

		source := ext.NewChanSource(in)
		flow1 := flow.NewMap(toUpper, 1)
		filter := flow.NewFilter(filterA, 1)
		sink := ext.NewChanSink(out)

		var _input = []string{"a", "b", "c"}
		var _expectedOutput = []string{"B", "C"}

		go ingest(_input, in)
		go deferClose(in, time.Second)
		go func() {
			fanOut := flow.RoundRobin(source.Via(filter).Via(flow1), 2)
			flow.Merge(fanOut...).To(sink)
		}()
    
		var _output []string
		for e := range sink.Out {
			_output = append(_output, e.(string))
		}
		sort.Strings(_output)
    
		assertEqual(t, _expectedOutput, _output)
	})
}

func TestSessionWindow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	sessionWindow := flow.NewSessionWindow(200 * time.Millisecond)
	sink := ext.NewChanSink(out)

	var _input = []string{"a", "b", "c"}
	go ingest(_input, in)
	go ingestDeferred("d", in, 300*time.Millisecond)
	go ingestDeferred("e", in, 700*time.Millisecond)
	go deferClose(in, time.Second)
	go func() {
		source.Via(sessionWindow).To(sink)
	}()

	var _output [][]interface{}
	for e := range sink.Out {
		_output = append(_output, e.([]interface{}))
	}

	assertEqual(t, len(_output), 3)
	assertEqual(t, len(_output[0]), 3)
	assertEqual(t, len(_output[1]), 1)
	assertEqual(t, len(_output[2]), 1)
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

	assertEqual(t, first.Msg.(int), 2)
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("%s != %s", a, b)
	}
}
