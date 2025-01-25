package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// TumblingWindow assigns each element to a window of a specified window size.
// Tumbling windows have a fixed size and do not overlap.
// T indicates the incoming element type, and the outgoing element type is []T.
type TumblingWindow[T any] struct {
	mu         sync.Mutex
	windowSize time.Duration
	in         chan any
	out        chan any
	done       chan struct{}
	buffer     []T
}

// Verify TumblingWindow satisfies the Flow interface.
var _ streams.Flow = (*TumblingWindow[any])(nil)

// NewTumblingWindow returns a new TumblingWindow operator.
// T specifies the incoming element type, and the outgoing element type is []T.
//
// size is the Duration of generated windows.
func NewTumblingWindow[T any](size time.Duration) *TumblingWindow[T] {
	tumblingWindow := &TumblingWindow[T]{
		windowSize: size,
		in:         make(chan any),
		out:        make(chan any),
		done:       make(chan struct{}),
	}
	go tumblingWindow.receive()
	go tumblingWindow.emit()

	return tumblingWindow
}

// Via asynchronously streams data to the given Flow and returns it.
func (tw *TumblingWindow[T]) Via(flow streams.Flow) streams.Flow {
	go tw.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (tw *TumblingWindow[T]) To(sink streams.Sink) {
	tw.transmit(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel of the TumblingWindow operator.
func (tw *TumblingWindow[T]) Out() <-chan any {
	return tw.out
}

// In returns the input channel of the TumblingWindow operator.
func (tw *TumblingWindow[T]) In() chan<- any {
	return tw.in
}

// transmit submits closed windows to the next Inlet.
func (tw *TumblingWindow[T]) transmit(inlet streams.Inlet) {
	for window := range tw.out {
		inlet.In() <- window
	}
	close(inlet.In())
}

// receive buffers the incoming elements.
func (tw *TumblingWindow[T]) receive() {
	for element := range tw.in {
		tw.mu.Lock()
		tw.buffer = append(tw.buffer, element.(T))
		tw.mu.Unlock()
	}
	close(tw.done)
}

// emit captures and emits a new window based on the fixed time interval.
func (tw *TumblingWindow[T]) emit() {
	ticker := time.NewTicker(tw.windowSize)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tw.dispatchWindow()

		case <-tw.done:
			tw.dispatchWindow()
			close(tw.out)
			return
		}
	}
}

// dispatchWindow creates a window from buffered elements and resets the buffer.
// It sends the slice of elements to the output channel if the window is not empty.
func (tw *TumblingWindow[T]) dispatchWindow() {
	tw.mu.Lock()
	windowElements := tw.buffer
	tw.buffer = nil
	tw.mu.Unlock()

	// send elements if the window is not empty
	if len(windowElements) > 0 {
		tw.out <- windowElements
	}
}
