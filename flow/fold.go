package flow

import (
	"github.com/reugn/go-streams"
)

// FoldFunction represents a Fold transformation function.
type FoldFunction[T, R any] func(T, R) R

// Fold implements a "rolling" fold transformation on a data stream with an
// initial value. Combines the current element with the last folded value and
// emits the new value.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//
// [ ---------- FoldFunction ---------- ]
//
// out -- 1' - 2' --- 3' - 4' ----- 5' -
type Fold[T, R any] struct {
	init         R
	foldFunction FoldFunction[T, R]
	in           chan any
	out          chan any
}

// Verify Fold satisfies the Flow interface.
var _ streams.Flow = (*Fold[any, any])(nil)

// NewFold returns a new Fold operator.
// T specifies the incoming element type, and the outgoing element type is R.
//
// init is the initial value for the folding process.
// foldFunction is the function that performs the fold transformation.
func NewFold[T, R any](init R, foldFunction FoldFunction[T, R]) *Fold[T, R] {
	foldFlow := &Fold[T, R]{
		init:         init,
		foldFunction: foldFunction,
		in:           make(chan any),
		out:          make(chan any),
	}

	// start processing stream elements
	go foldFlow.stream()

	return foldFlow
}

// Via asynchronously streams data to the given Flow and returns it.
func (m *Fold[T, R]) Via(flow streams.Flow) streams.Flow {
	go m.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (m *Fold[T, R]) To(sink streams.Sink) {
	m.transmit(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel of the Fold operator.
func (m *Fold[T, R]) Out() <-chan any {
	return m.out
}

// In returns the input channel of the Fold operator.
func (m *Fold[T, R]) In() chan<- any {
	return m.in
}

func (m *Fold[T, R]) transmit(inlet streams.Inlet) {
	for element := range m.Out() {
		inlet.In() <- element
	}
	close(inlet.In())
}

// stream consumes elements from the input channel, applies the foldFunction to
// each element along with the previously accumulated value, and emits the updated
// value into the output channel. All input elements are assumed to be of type T.
// The processing is done sequentially, ensuring that the order of accumulation is
// maintained.
func (m *Fold[T, R]) stream() {
	lastFolded := m.init
	for element := range m.in {
		lastFolded = m.foldFunction(element.(T), lastFolded)
		m.out <- lastFolded
	}
	close(m.out)
}
