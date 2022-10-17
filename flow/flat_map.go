package flow

import (
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
	in              chan interface{}
	out             chan interface{}
	parallelism     uint
}

// Verify FlatMap satisfies the Flow interface.
var _ streams.Flow = (*FlatMap[any, any])(nil)

// NewFlatMap returns a new FlatMap instance.
//
// flatMapFunction is the FlatMap transformation function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewFlatMap[T, R any](flatMapFunction FlatMapFunction[T, R], parallelism uint) *FlatMap[T, R] {
	flatMap := &FlatMap[T, R]{
		flatMapFunction: flatMapFunction,
		in:              make(chan interface{}),
		out:             make(chan interface{}),
		parallelism:     parallelism,
	}
	go flatMap.doStream()

	return flatMap
}

// Via streams data through the given flow
func (fm *FlatMap[T, R]) Via(flow streams.Flow) streams.Flow {
	go fm.transmit(flow)
	return flow
}

// To streams data to the given sink
func (fm *FlatMap[T, R]) To(sink streams.Sink) {
	fm.transmit(sink)
}

// Out returns an output channel for sending data
func (fm *FlatMap[T, R]) Out() <-chan interface{} {
	return fm.out
}

// In returns an input channel for receiving data
func (fm *FlatMap[T, R]) In() chan<- interface{} {
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
	for i := 0; i < int(fm.parallelism); i++ {
		sem <- struct{}{}
	}
	close(fm.out)
}
