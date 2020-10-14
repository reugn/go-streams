package flow

import (
	"github.com/reugn/go-streams"
)

// FilterFunc is a filter predicate function.
type FilterFunc func(interface{}) bool

// Filter filters the incoming elements using a predicate.
// If the predicate returns true the element is passed downstream,
// if it returns false the element is discarded.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
//    [---------- FilterFunc -----------]
//        |    |                    |
// out -- 1 -- 2 ------------------ 5 --
type Filter struct {
	FilterF     FilterFunc
	in          chan interface{}
	out         chan interface{}
	parallelism uint
}

// Verify Filter satisfies the Flow interface.
var _ streams.Flow = (*Filter)(nil)

// NewFilter returns a new Filter instance.
// filterFunc is the filter predicate function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewFilter(filterFunc FilterFunc, parallelism uint) *Filter {
	filter := &Filter{
		filterFunc,
		make(chan interface{}),
		make(chan interface{}),
		parallelism,
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

// throws items that are not satisfying the filter function
func (f *Filter) doStream() {
	sem := make(chan struct{}, f.parallelism)
	for elem := range f.in {
		sem <- struct{}{}
		go func(e interface{}) {
			defer func() { <-sem }()
			if f.FilterF(e) {
				f.out <- e
			}
		}(elem)
	}
	for i := 0; i < int(f.parallelism); i++ {
		sem <- struct{}{}
	}
	close(f.out)
}
