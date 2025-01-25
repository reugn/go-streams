package flow

import (
	"fmt"

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
// filterPredicate is the boolean-valued filter function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
// If the parallelism argument is not positive, NewFilter will panic.
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
	go filter.doStream()

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

// doStream discards items that don't match the filter predicate.
func (f *Filter[T]) doStream() {
	sem := make(chan struct{}, f.parallelism)
	for elem := range f.in {
		sem <- struct{}{}
		go func(element T) {
			defer func() { <-sem }()
			if f.filterPredicate(element) {
				f.out <- element
			}
		}(elem.(T))
	}
	for i := 0; i < f.parallelism; i++ {
		sem <- struct{}{}
	}
	close(f.out)
}
