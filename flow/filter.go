package flow

import (
	"github.com/reugn/go-streams"
)

// FilterFunc resolver
type FilterFunc func(interface{}) bool

// Filter stream flow
type Filter struct {
	FilterF     FilterFunc
	in          chan interface{}
	parallelism uint
}

// NewFilter returns new Filter instance
// FilterFunc - resolver function
// parallelism - parallelism factor, in case events order matters use parallelism = 1
func NewFilter(f FilterFunc, parallelism uint) *Filter {
	return &Filter{
		f,
		make(chan interface{}),
		parallelism,
	}
}

// Via streams data through given flow
func (f *Filter) Via(flow streams.Flow) streams.Flow {
	go f.doStream(flow)
	return flow
}

// To streams data to given sink
func (f *Filter) To(sink streams.Sink) {
	f.doStream(sink)
}

// Out returns channel for sending data
func (f *Filter) Out() <-chan interface{} {
	return f.in
}

// In returns channel for receiving data
func (f *Filter) In() chan<- interface{} {
	return f.in
}

// throws items not satisfying filter function
func (f *Filter) doStream(inlet streams.Inlet) {
	sem := make(chan struct{}, f.parallelism)
	for elem := range f.in {
		sem <- struct{}{}
		go func(e interface{}) {
			defer func() { <-sem }()
			if f.FilterF(e) {
				inlet.In() <- e
			}
		}(elem)
	}
	for i := 0; i < int(f.parallelism); i++ {
		sem <- struct{}{}
	}
	close(inlet.In())
}
