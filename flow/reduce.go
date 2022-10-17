package flow

import (
	"github.com/reugn/go-streams"
)

// ReduceFunction combines the current element with the last reduced value.
type ReduceFunction[T any] func(T, T) T

// Reduce represents a “rolling” reduce on a data stream.
// Combines the current element with the last reduced value and emits the new value.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// [ --------- ReduceFunction --------- ]
//
// out -- 1 -- 2' --- 3' - 4' ----- 5' -
type Reduce[T any] struct {
	reduceFunction ReduceFunction[T]
	in             chan interface{}
	out            chan interface{}
	lastReduced    interface{}
}

// Verify Reduce satisfies the Flow interface.
var _ streams.Flow = (*Reduce[any])(nil)

// NewReduce returns a new Reduce instance.
//
// reduceFunction combines the current element with the last reduced value.
func NewReduce[T any](reduceFunction ReduceFunction[T]) *Reduce[T] {
	reduce := &Reduce[T]{
		reduceFunction: reduceFunction,
		in:             make(chan interface{}),
		out:            make(chan interface{}),
	}
	go reduce.doStream()
	return reduce
}

// Via streams data through the given flow
func (r *Reduce[T]) Via(flow streams.Flow) streams.Flow {
	go r.transmit(flow)
	return flow
}

// To streams data to the given sink
func (r *Reduce[T]) To(sink streams.Sink) {
	r.transmit(sink)
}

// Out returns an output channel for sending data
func (r *Reduce[T]) Out() <-chan interface{} {
	return r.out
}

// In returns an input channel for receiving data
func (r *Reduce[T]) In() chan<- interface{} {
	return r.in
}

func (r *Reduce[T]) transmit(inlet streams.Inlet) {
	for element := range r.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

func (r *Reduce[T]) doStream() {
	for element := range r.in {
		if r.lastReduced == nil {
			r.lastReduced = element
		} else {
			r.lastReduced = r.reduceFunction(r.lastReduced.(T), element.(T))
		}
		r.out <- r.lastReduced
	}
	close(r.out)
}
