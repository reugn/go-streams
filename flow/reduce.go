package flow

import (
	"github.com/reugn/go-streams"
)

// ReduceFunction combines the current element with the last reduced value.
type ReduceFunction[T any] func(T, T) T

// Reduce implements a “rolling” reduce transformation on a data stream.
// Combines the current element with the last reduced value and emits the new value.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// [ --------- ReduceFunction --------- ]
//
// out -- 1 -- 2' --- 3' - 4' ----- 5' -
type Reduce[T any] struct {
	reduceFunction ReduceFunction[T]
	in             chan any
	out            chan any
}

// Verify Reduce satisfies the Flow interface.
var _ streams.Flow = (*Reduce[any])(nil)

// NewReduce returns a new Reduce operator.
// T specifies the incoming and the outgoing element type.
//
// reduceFunction combines the current element with the last reduced value.
func NewReduce[T any](reduceFunction ReduceFunction[T]) *Reduce[T] {
	reduce := &Reduce[T]{
		reduceFunction: reduceFunction,
		in:             make(chan any),
		out:            make(chan any),
	}
	go reduce.doStream()

	return reduce
}

// Via asynchronously streams data to the given Flow and returns it.
func (r *Reduce[T]) Via(flow streams.Flow) streams.Flow {
	go r.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (r *Reduce[T]) To(sink streams.Sink) {
	r.transmit(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel of the Reduce operator.
func (r *Reduce[T]) Out() <-chan any {
	return r.out
}

// In returns the input channel of the Reduce operator.
func (r *Reduce[T]) In() chan<- any {
	return r.in
}

func (r *Reduce[T]) transmit(inlet streams.Inlet) {
	for element := range r.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

func (r *Reduce[T]) doStream() {
	var lastReduced any
	for element := range r.in {
		if lastReduced == nil {
			lastReduced = element
		} else {
			lastReduced = r.reduceFunction(lastReduced.(T), element.(T))
		}
		r.out <- lastReduced
	}
	close(r.out)
}
