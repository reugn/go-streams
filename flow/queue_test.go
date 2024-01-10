package flow_test

import (
	"container/heap"
	"testing"
	"time"

	"github.com/reugn/go-streams/flow"
)

func TestQueueOps(t *testing.T) {
	queue := &flow.PriorityQueue{}
	heap.Push(queue, flow.NewItem(1, time.Now().UnixNano(), 0))
	heap.Push(queue, flow.NewItem(2, 1234, 0))
	heap.Push(queue, flow.NewItem(3, time.Now().UnixNano(), 0))
	queue.Swap(0, 1)
	head := queue.Head()
	queue.Update(head, time.Now().UnixNano())
	first := heap.Pop(queue).(*flow.Item)

	assertEquals(t, 2, first.Msg.(int))
}

func TestQueueOrder(t *testing.T) {
	queue := &flow.PriorityQueue{}

	pushItem(queue, 5)
	pushItem(queue, 4)
	pushItem(queue, 6)
	pushItem(queue, 3)
	pushItem(queue, 7)
	pushItem(queue, 2)
	pushItem(queue, 8)
	pushItem(queue, 1)
	pushItem(queue, 9)

	assertEquals(t, 1, popMsg(queue))
	assertEquals(t, 2, popMsg(queue))
	assertEquals(t, 3, popMsg(queue))
	assertEquals(t, 4, popMsg(queue))
	assertEquals(t, 5, popMsg(queue))
	assertEquals(t, 6, popMsg(queue))
	assertEquals(t, 7, popMsg(queue))
	assertEquals(t, 8, popMsg(queue))
	assertEquals(t, 9, popMsg(queue))
}

func pushItem(queue *flow.PriorityQueue, timestamp int64) {
	item := flow.NewItem(timestamp, timestamp, 0)
	heap.Push(queue, item)
}

func popMsg(queue *flow.PriorityQueue) int64 {
	return (heap.Pop(queue).(*flow.Item)).Msg.(int64)
}
