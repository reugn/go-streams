package flow

import (
	"fmt"
	"sync"

	"github.com/reugn/go-streams"
)

// MapFunction represents a Map transformation function.
type MapFunction[T, R any] func(T) R

// Map takes one element and produces one element.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// [ ---------- MapFunction ---------- ]
//
// out -- 1' - 2' --- 3' - 4' ----- 5' -
type Map[T, R any] struct {
	mapFunction MapFunction[T, R]
	in          chan any
	out         chan any
	parallelism int
}

// Verify Map satisfies the Flow interface.
var _ streams.Flow = (*Map[any, any])(nil)

// NewMap returns a new Map operator.
// T specifies the incoming element type, and the outgoing element type is R.
//
// mapFunction is the Map transformation function.
// parallelism specifies the number of goroutines to use for parallel processing. If
// the order of elements in the output stream must be preserved, set parallelism to 1.
//
// NewMap will panic if parallelism is less than 1.
func NewMap[T, R any](mapFunction MapFunction[T, R], parallelism int) *Map[T, R] {
	if parallelism < 1 {
		panic(fmt.Sprintf("nonpositive Map parallelism: %d", parallelism))
	}

	mapFlow := &Map[T, R]{
		mapFunction: mapFunction,
		in:          make(chan any),
		out:         make(chan any),
		parallelism: parallelism,
	}

	// start processing stream elements
	go mapFlow.stream()

	return mapFlow
}

// Via asynchronously streams data to the given Flow and returns it.
func (m *Map[T, R]) Via(flow streams.Flow) streams.Flow {
	go m.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (m *Map[T, R]) To(sink streams.Sink) {
	m.transmit(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel of the Map operator.
func (m *Map[T, R]) Out() <-chan any {
	return m.out
}

// In returns the input channel of the Map operator.
func (m *Map[T, R]) In() chan<- any {
	return m.in
}

func (m *Map[T, R]) transmit(inlet streams.Inlet) {
	for element := range m.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

// stream reads elements from the input channel, applies the mapFunction
// to each element, and sends the resulting element to the output channel.
// It uses a pool of goroutines to process elements in parallel.
func (m *Map[T, R]) stream() {
	var wg sync.WaitGroup
	// create a pool of worker goroutines
	for i := 0; i < m.parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for element := range m.in {
				m.out <- m.mapFunction(element.(T))
			}
		}()
	}

	// wait for worker goroutines to finish processing inbound elements
	wg.Wait()
	// close the output channel
	close(m.out)
}
