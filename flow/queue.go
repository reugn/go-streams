package flow

import "container/heap"

// Item represents a PriorityQueue item.
type Item struct {
	Msg   interface{}
	epoch int64 // item priority, backed by the epoch time.
	index int   // maintained by the heap.Interface methods.
}

// NewItem returns a new Item.
func NewItem(msg interface{}, epoch int64, index int) *Item {
	return &Item{msg, epoch, index}
}

// PriorityQueue implements heap.Interface.
type PriorityQueue []*Item

// Len returns the PriorityQueue length.
func (pq PriorityQueue) Len() int { return len(pq) }

// Less is the items less comparator.
func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].epoch < pq[j].epoch
}

// Swap exchanges indexes of the items.
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push implements heap.Interface.Push.
// Appends an item to the PriorityQueue.
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

// Pop implements heap.Interface.Pop.
// Removes and returns the Len() - 1 element.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Head returns the first item of the PriorityQueue without removing it.
func (pq *PriorityQueue) Head() *Item {
	return (*pq)[0]
}

// Update sets item priority and calls heap.Fix to re-establish the heap ordering.
func (pq *PriorityQueue) Update(item *Item, newEpoch int64) {
	item.epoch = newEpoch
	heap.Fix(pq, item.index)
}

// Slice returns a sliced PriorityQueue using the given bounds.
func (pq PriorityQueue) Slice(start, end int) PriorityQueue {
	return pq[start:end]
}
