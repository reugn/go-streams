package flow

import (
	"time"

	"github.com/reugn/go-streams"
)

// Batch processor breaks a stream of elements into batches based on size or timing.
// When the maximum batch size is reached or the batch time is elapsed, and the current buffer
// is not empty, a new batch will be emitted.
// Note: once a batch is sent downstream, the timer will be reset.
type Batch struct {
	maxBatchSize int
	timeInterval time.Duration
	in           chan interface{}
	out          chan interface{}
}

// Verify Batch satisfies the Flow interface.
var _ streams.Flow = (*Batch)(nil)

// NewBatch returns a new Batch instance using the specified maximum batch size and the
// time interval.
// NewBatch will panic if the maxBatchSize argument is not positive.
func NewBatch(maxBatchSize int, timeInterval time.Duration) *Batch {
	if maxBatchSize < 1 {
		panic("maxBatchSize must be positive")
	}
	batchFlow := &Batch{
		maxBatchSize: maxBatchSize,
		timeInterval: timeInterval,
		in:           make(chan interface{}),
		out:          make(chan interface{}),
	}
	go batchFlow.batchStream()

	return batchFlow
}

// Via streams data through the given flow
func (b *Batch) Via(flow streams.Flow) streams.Flow {
	go b.transmit(flow)
	return flow
}

// To streams data to the given sink
func (b *Batch) To(sink streams.Sink) {
	b.transmit(sink)
}

// Out returns the output channel of the Batch.
func (b *Batch) Out() <-chan interface{} {
	return b.out
}

// In returns the input channel of the Batch.
func (b *Batch) In() chan<- interface{} {
	return b.in
}

// transmit submits batches of elements to the next Inlet.
func (b *Batch) transmit(inlet streams.Inlet) {
	for batch := range b.out {
		inlet.In() <- batch
	}
	close(inlet.In())
}

// batchStream buffers the incoming stream and emits a batch of elements
// if the maximum batch size reached or the batch times out.
func (b *Batch) batchStream() {
	ticker := time.NewTicker(b.timeInterval)
	defer ticker.Stop()

	batch := make([]interface{}, 0, b.maxBatchSize)
	for {
		select {
		case element, ok := <-b.in:
			if ok {
				batch = append(batch, element)
				// dispatch the batch if the maximum batch size has been reached
				if len(batch) >= b.maxBatchSize {
					b.out <- batch
					batch = make([]interface{}, 0, b.maxBatchSize)
				}
				// reset the ticker
				ticker.Reset(b.timeInterval)
			} else {
				// send the available buffer elements as a new batch, close the
				// output channel and return
				if len(batch) > 0 {
					b.out <- batch
				}
				close(b.out)
				return
			}
		case <-ticker.C:
			// timeout; dispatch and reset the buffer
			if len(batch) > 0 {
				b.out <- batch
				batch = make([]interface{}, 0, b.maxBatchSize)
			}
		}
	}
}
