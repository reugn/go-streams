package flow

import (
	"sync"
	"time"

	"github.com/reugn/go-streams"
)

// TumblingWindow flow
// size - The size of the generated windows
type TumblingWindow struct {
	sync.Mutex
	size   time.Duration
	in     chan interface{}
	out    chan interface{}
	buffer []interface{}
}

func NewTumblingWindow(size time.Duration) *TumblingWindow {
	window := &TumblingWindow{
		size: size,
		in:   make(chan interface{}),
		out:  make(chan interface{}), //windows channel
	}
	go window.receive()
	go window.emit()
	return window
}

func (tw *TumblingWindow) Via(flow streams.Flow) streams.Flow {
	go tw.transmit(flow)
	return flow
}

func (tw *TumblingWindow) To(sink streams.Sink) {
	tw.transmit(sink)
}

func (tw *TumblingWindow) Out() <-chan interface{} {
	return tw.out
}

func (tw *TumblingWindow) In() chan<- interface{} {
	return tw.in
}

// retransmit emitted window to the next Inlet
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
	close(tw.out)
}

func (tw *TumblingWindow) emit() {
	for {
		select {
		case <-time.After(tw.size):
			tw.Lock()
			windowSlice := append(tw.buffer[:0:0], tw.buffer...)
			tw.buffer = nil
			tw.Unlock()
			//send to out chan
			if len(windowSlice) > 0 {
				tw.out <- windowSlice
			}
		}
	}
}
