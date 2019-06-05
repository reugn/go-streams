package flow

import "container/heap"

// Item of PriorityQueue
type Item struct {
	Msg   interface{}
	epoch int64 // item priority by epoch time
	index int   // maintained by the heap.Interface methods
}

// NewItem constructor
func NewItem(msg interface{}, epoch int64, index int) *Item {
	return &Item{msg, epoch, index}
}

// PriorityQueue implements heap.Interface
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].epoch < pq[j].epoch
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Head() *Item {
	return (*pq)[0]
}

func (pq *PriorityQueue) Update(item *Item, newEpoch int64) {
	item.epoch = newEpoch
	heap.Fix(pq, item.index)
}

func (pq PriorityQueue) Slice(start, end int) PriorityQueue {
	return pq[start:end]
}
