package flow

import (
	"fmt"
	"sync"

	"github.com/reugn/go-streams"
)

// FilterPredicate represents a filter predicate (boolean-valued function).
type FilterPredicate[T any] func(T) bool

// Filter filters incoming elements using a filter predicate.
// If an element matches the predicate, the element is passed downstream.
// If not, the element is discarded.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// [ -------- FilterPredicate -------- ]
//
// out -- 1 -- 2 ------------------ 5 --
type Filter[T any] struct {
	filterPredicate FilterPredicate[T]
	in              chan any
	out             chan any
	parallelism     int
}

// Verify Filter satisfies the Flow interface.
var _ streams.Flow = (*Filter[any])(nil)

// NewFilter returns a new Filter operator.
// T specifies the incoming and the outgoing element type.
//
// filterPredicate is a function that accepts an element of type T and returns true
// if the element should be included in the output stream, and false if it should be
// filtered out.
// parallelism specifies the number of goroutines to use for parallel processing. If
// the order of elements in the output stream must be preserved, set parallelism to 1.
//
// NewFilter will panic if parallelism is less than 1.
func NewFilter[T any](filterPredicate FilterPredicate[T], parallelism int) *Filter[T] {
	if parallelism < 1 {
		panic(fmt.Sprintf("nonpositive Filter parallelism: %d", parallelism))
	}

	filter := &Filter[T]{
		filterPredicate: filterPredicate,
		in:              make(chan any),
		out:             make(chan any),
		parallelism:     parallelism,
	}

	// start processing stream elements
	go filter.stream()

	return filter
}

// Via asynchronously streams data to the given Flow and returns it.
func (f *Filter[T]) Via(flow streams.Flow) streams.Flow {
	go f.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (f *Filter[T]) To(sink streams.Sink) {
	f.transmit(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel of the Filter operator.
func (f *Filter[T]) Out() <-chan any {
	return f.out
}

// In returns the input channel of the Filter operator.
func (f *Filter[T]) In() chan<- any {
	return f.in
}

func (f *Filter[T]) transmit(inlet streams.Inlet) {
	for element := range f.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

// stream reads elements from the input channel, filters them using the
// filterPredicate, and sends the filtered elements to the output channel.
// It uses a pool of goroutines to process elements in parallel.
func (f *Filter[T]) stream() {
	var wg sync.WaitGroup
	// create a pool of worker goroutines
	for i := 0; i < f.parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for element := range f.in {
				if f.filterPredicate(element.(T)) {
					f.out <- element
				}
			}
		}()
	}

	// wait for worker goroutines to finish processing inbound elements
	wg.Wait()
	// close the output channel
	close(f.out)
}
