package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// TumblingWindow assigns each element to a window of a specified window size.
// Tumbling windows have a fixed size and do not overlap.
type TumblingWindow struct {
	sync.Mutex
	size   time.Duration
	in     chan interface{}
	out    chan interface{}
	done   chan struct{}
	buffer []interface{}
}

// Verify TumblingWindow satisfies the Flow interface.
var _ streams.Flow = (*TumblingWindow)(nil)

// NewTumblingWindow returns a new TumblingWindow instance.
// size is the size of the generated windows.
func NewTumblingWindow(size time.Duration) *TumblingWindow {
	window := &TumblingWindow{
		size: size,
		in:   make(chan interface{}),
		out:  make(chan interface{}), // windows channel
		done: make(chan struct{}),
	}
	go window.receive()
	go window.emit()
	return window
}

// Via streams data through the given flow
func (tw *TumblingWindow) Via(flow streams.Flow) streams.Flow {
	go tw.transmit(flow)
	return flow
}

// To streams data to the given sink
func (tw *TumblingWindow) To(sink streams.Sink) {
	tw.transmit(sink)
}

// Out returns an output channel for sending data
func (tw *TumblingWindow) Out() <-chan interface{} {
	return tw.out
}

// In returns an input channel for receiving data
func (tw *TumblingWindow) In() chan<- interface{} {
	return tw.in
}

// submit emitted windows to the next Inlet
func (tw *TumblingWindow) transmit(inlet streams.Inlet) {
	for elem := range tw.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (tw *TumblingWindow) receive() {
	for elem := range tw.in {
		tw.Lock()
		tw.buffer = append(tw.buffer, elem)
		tw.Unlock()
	}
	close(tw.done)
	close(tw.out)
}

// generate and emit a window
func (tw *TumblingWindow) emit() {
	for {
		select {
		case <-time.After(tw.size):
			tw.Lock()
			windowSlice := append(tw.buffer[:0:0], tw.buffer...)
			tw.buffer = nil
			tw.Unlock()
			// send window slice to the out chan
			if len(windowSlice) > 0 {
				tw.out <- windowSlice
			}

		case <-tw.done:
			return
		}
	}
}
