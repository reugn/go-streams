package flow

import (
	"fmt"

	"github.com/reugn/go-streams"
)

// FlatMapFunction represents a FlatMap transformation function.
type FlatMapFunction[T, R any] func(T) []R

// FlatMap takes one element and produces zero, one, or more elements.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// [ -------- FlatMapFunction -------- ]
//
// out -- 1' - 2' -------- 4'- 4" - 5' -
type FlatMap[T, R any] struct {
	flatMapFunction FlatMapFunction[T, R]
	in              chan any
	out             chan any
	parallelism     int
}

// Verify FlatMap satisfies the Flow interface.
var _ streams.Flow = (*FlatMap[any, any])(nil)

// NewFlatMap returns a new FlatMap operator.
// T specifies the incoming element type, and the outgoing element type is []R.
//
// flatMapFunction is the FlatMap transformation function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
// If the parallelism argument is not positive, NewFlatMap will panic.
func NewFlatMap[T, R any](flatMapFunction FlatMapFunction[T, R], parallelism int) *FlatMap[T, R] {
	if parallelism < 1 {
		panic(fmt.Sprintf("nonpositive FlatMap parallelism: %d", parallelism))
	}
	flatMap := &FlatMap[T, R]{
		flatMapFunction: flatMapFunction,
		in:              make(chan any),
		out:             make(chan any),
		parallelism:     parallelism,
	}
	go flatMap.doStream()

	return flatMap
}

// Via asynchronously streams data to the given Flow and returns it.
func (fm *FlatMap[T, R]) Via(flow streams.Flow) streams.Flow {
	go fm.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (fm *FlatMap[T, R]) To(sink streams.Sink) {
	fm.transmit(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel of the FlatMap operator.
func (fm *FlatMap[T, R]) Out() <-chan any {
	return fm.out
}

// In returns the input channel of the FlatMap operator.
func (fm *FlatMap[T, R]) In() chan<- any {
	return fm.in
}

func (fm *FlatMap[T, R]) transmit(inlet streams.Inlet) {
	for element := range fm.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

func (fm *FlatMap[T, R]) doStream() {
	sem := make(chan struct{}, fm.parallelism)
	for elem := range fm.in {
		sem <- struct{}{}
		go func(element T) {
			defer func() { <-sem }()
			result := fm.flatMapFunction(element)
			for _, item := range result {
				fm.out <- item
			}
		}(elem.(T))
	}
	for i := 0; i < fm.parallelism; i++ {
		sem <- struct{}{}
	}
	close(fm.out)
}
