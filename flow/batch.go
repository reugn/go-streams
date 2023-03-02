package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// Batch batches the incoming elements into a slice by size or time.
// If the batch size is reached or the batch time is elapsed,
// and the current batch is not empty, it will be emitted.
// Note: once the batch is emitted, the timer will be reset.
//
// in: a(10ms), b(15ms), c(20ms), d(40ms), e(110ms), f(300ms)
//
//	-------------- NewBatch(3, 100ms) --------------------
//
// out:
//
//	[a b c] (max size, timer reset at 20ms)
//	[d e] (max time, timer reset at 120ms) (between 120ms and 220ms, there is no element, so there is no batch)
//	[f] (max time, timer reset at 320ms)
type Batch struct {
	maxBatchSize int
	maxWaitTime  time.Duration
	ticker       *time.Ticker
	in           chan any
	out          chan any
	buffer       []any
	mu           sync.Mutex
	done         chan struct{}
}

// Verify Batch satisfies the Flow interface
var _ streams.Flow = (*Batch)(nil)

// NewBatch returns a new Batch instance
//
// it will batch the incoming elements into a slice
// whether the batch size is reached or the time is elapsed.
func NewBatch(maxBatchSize int, maxWaitTime time.Duration) *Batch {
	if maxBatchSize <= 0 {
		panic("batch size should greater than 0")
	}

	bf := &Batch{
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		ticker:       time.NewTicker(maxWaitTime),
		buffer:       make([]any, 0, maxBatchSize),
		in:           make(chan any),
		out:          make(chan any),
		done:         make(chan struct{}),
	}
	go bf.batchBySize()
	go bf.batchByTime()

	return bf
}

// Via sends the flow to the next stage via specified flow
func (bf *Batch) Via(flow streams.Flow) streams.Flow {
	go bf.transmit(flow)

	return flow
}

// To sends the flow to the given sink
func (bf *Batch) To(sink streams.Sink) {
	bf.transmit(sink)
}

// Out returns the output channel of the Batch
func (bf *Batch) Out() <-chan any {
	return bf.out
}

// In returns the input channel of the Batch
func (bf *Batch) In() chan<- any {
	return bf.in
}

func (bf *Batch) batchBySize() {
	for elem := range bf.in {
		bf.mu.Lock()
		bf.buffer = append(bf.buffer, elem)
		bf.mu.Unlock()
		if len(bf.buffer) >= bf.maxBatchSize {
			bf.flush()
		}
	}
	close(bf.done)
	close(bf.out)
}

func (bf *Batch) batchByTime() {
	defer bf.ticker.Stop()

	for {
		select {
		case <-bf.ticker.C:
			bf.flush()
		case <-bf.done:
			return
		}
	}
}

// flush sends the batched items to the next flow
func (bf *Batch) flush() {
	bf.mu.Lock()
	buffer := bf.buffer
	bf.buffer = make([]any, 0, bf.maxBatchSize)
	bf.ticker.Reset(bf.maxWaitTime)
	bf.mu.Unlock()

	if len(buffer) > 0 {
		bf.out <- buffer
	}
}

func (bf *Batch) transmit(inlet streams.Inlet) {
	for item := range bf.out {
		inlet.In() <- item
	}
	defer close(inlet.In())
}
