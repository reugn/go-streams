package flow

import (
	"github.com/reugn/go-streams"
)

// FilterPredicate represents a filter predicate (boolean-valued function).
type FilterPredicate func(interface{}) bool

// Filter filters incoming elements using a filter predicate.
// If an element matches the predicate, the element is passed downstream.
// If not, the element is discarded.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// [ -------- FilterPredicate -------- ]
//
// out -- 1 -- 2 ------------------ 5 --
type Filter struct {
	filterPredicate FilterPredicate
	in              chan interface{}
	out             chan interface{}
	parallelism     uint
}

// Verify Filter satisfies the Flow interface.
var _ streams.Flow = (*Filter)(nil)

// NewFilter returns a new Filter instance.
//
// filterPredicate is the boolean-valued filter function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewFilter(filterPredicate FilterPredicate, parallelism uint) *Filter {
	filter := &Filter{
		filterPredicate: filterPredicate,
		in:              make(chan interface{}),
		out:             make(chan interface{}),
		parallelism:     parallelism,
	}
	go filter.doStream()

	return filter
}

// Via streams data through the given flow
func (f *Filter) Via(flow streams.Flow) streams.Flow {
	go f.transmit(flow)
	return flow
}

// To streams data to the given sink
func (f *Filter) To(sink streams.Sink) {
	f.transmit(sink)
}

// Out returns an output channel for sending data
func (f *Filter) Out() <-chan interface{} {
	return f.out
}

// In returns an input channel for receiving data
func (f *Filter) In() chan<- interface{} {
	return f.in
}

func (f *Filter) transmit(inlet streams.Inlet) {
	for elem := range f.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

// doStream discards items that don't match the filter predicate.
func (f *Filter) doStream() {
	sem := make(chan struct{}, f.parallelism)
	for elem := range f.in {
		sem <- struct{}{}
		go func(e interface{}) {
			defer func() { <-sem }()
			if f.filterPredicate(e) {
				f.out <- e
			}
		}(elem)
	}
	for i := 0; i < int(f.parallelism); i++ {
		sem <- struct{}{}
	}
	close(f.out)
}
