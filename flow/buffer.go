package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// NewBuffer create a new Buffer instance
// timeout is a given time interval for a timer that used to control the emits frequency
// size is the maximum number of data in the buffer slice each time
// after each send, the timer is reset
func NewBuffer[T any](timeout time.Duration, size, parallelism uint) *Buffer[T] {
	batch := &Buffer[T]{
		timeout:     timeout,
		size:        size,
		in:          make(chan interface{}),
		out:         make(chan interface{}),
		parallelism: parallelism,
	}
	go batch.doStream()
	return batch
}

// Buffer emits buffers of items it collects from the source
// either from a given count or at a given time interval.
type Buffer[T any] struct {
	timeout     time.Duration
	size        uint
	in          chan interface{}
	out         chan interface{}
	parallelism uint
}

// Via streams data through the given flow
func (m *Buffer[T]) Via(flow streams.Flow) streams.Flow {
	go m.transmit(flow)
	return flow
}

// To streams data to the given sink
func (m *Buffer[T]) To(sink streams.Sink) {
	m.transmit(sink)
}

// Out returns an output channel for sending data
func (m *Buffer[T]) Out() <-chan interface{} {
	return m.out
}

// In returns an input channel for receiving data
func (m *Buffer[T]) In() chan<- interface{} {
	return m.in
}

func (m *Buffer[T]) transmit(inlet streams.Inlet) {
	for elem := range m.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (m *Buffer[T]) doStream() {
	wg := new(sync.WaitGroup)
	tick := time.NewTicker(m.timeout)
	defer tick.Stop()

	for i := 0; i < int(m.parallelism); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			batches := make([]T, 0, m.size)
			defer func() {
				if len(batches) > 0 {
					m.out <- batches
				}
			}()

			for {
				select {
				case v, ok := <-m.in:
					if ok {
						batches = append(batches, v.(T))
						if len(batches) >= int(m.size) {
							m.out <- batches
							tick.Reset(m.timeout)
							batches = make([]T, 0, m.size)
						}
					} else {
						return
					}
				case <-tick.C:
					if len(batches) > 0 {
						m.out <- batches
						batches = make([]T, 0, m.size)
					}
				}
			}
		}()
	}
	wg.Wait()
	close(m.out)
}
