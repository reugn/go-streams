package flow

import (
	"fmt"

	"github.com/reugn/go-streams"
)

// MapFunction represents a Map transformation function.
type MapFunction[T, R any] func(T) R

// Map takes one element and produces one element.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// [ ---------- MapFunction ---------- ]
//
// out -- 1' - 2' --- 3' - 4' ----- 5' -
type Map[T, R any] struct {
	mapFunction MapFunction[T, R]
	in          chan any
	out         chan any
	parallelism int
}

// Verify Map satisfies the Flow interface.
var _ streams.Flow = (*Map[any, any])(nil)

// NewMap returns a new Map operator.
// T specifies the incoming element type, and the outgoing element type is R.
//
// mapFunction is the Map transformation function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
// If the parallelism argument is not positive, NewMap will panic.
func NewMap[T, R any](mapFunction MapFunction[T, R], parallelism int) *Map[T, R] {
	if parallelism < 1 {
		panic(fmt.Sprintf("nonpositive Map parallelism: %d", parallelism))
	}
	mapFlow := &Map[T, R]{
		mapFunction: mapFunction,
		in:          make(chan any),
		out:         make(chan any),
		parallelism: parallelism,
	}
	go mapFlow.doStream()
	return mapFlow
}

// Via streams data to a specified Flow and returns it.
func (m *Map[T, R]) Via(flow streams.Flow) streams.Flow {
	go m.transmit(flow)
	return flow
}

// To streams data to a specified Sink.
func (m *Map[T, R]) To(sink streams.Sink) {
	m.transmit(sink)
}

// Out returns the output channel of the Map operator.
func (m *Map[T, R]) Out() <-chan any {
	return m.out
}

// In returns the input channel of the Map operator.
func (m *Map[T, R]) In() chan<- any {
	return m.in
}

func (m *Map[T, R]) transmit(inlet streams.Inlet) {
	for element := range m.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

func (m *Map[T, R]) doStream() {
	sem := make(chan struct{}, m.parallelism)
	for elem := range m.in {
		sem <- struct{}{}
		go func(element T) {
			defer func() { <-sem }()
			result := m.mapFunction(element)
			m.out <- result
		}(elem.(T))
	}
	for i := 0; i < m.parallelism; i++ {
		sem <- struct{}{}
	}
	close(m.out)
}
