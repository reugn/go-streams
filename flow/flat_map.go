package flow

import (
	"fmt"
	"sync"

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
// parallelism specifies the number of goroutines to use for parallel processing. If
// the order of elements in the output stream must be preserved, set parallelism to 1.
//
// NewFlatMap will panic if parallelism is less than 1.
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

	// start processing stream elements
	go flatMap.stream()

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

// stream reads elements from the input channel, applies the flatMapFunction
// to each element, and sends the resulting elements to the output channel.
// It uses a pool of goroutines to process elements in parallel.
func (fm *FlatMap[T, R]) stream() {
	var wg sync.WaitGroup
	// create a pool of worker goroutines
	for i := 0; i < fm.parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for element := range fm.in {
				result := fm.flatMapFunction(element.(T))
				for _, item := range result {
					fm.out <- item
				}
			}
		}()
	}

	// wait for worker goroutines to finish processing inbound elements
	wg.Wait()
	// close the output channel
	close(fm.out)
}
