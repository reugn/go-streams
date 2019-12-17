package flow

import (
	"github.com/reugn/go-streams"
)

// PassThrough flow
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
// out -- 1 -- 2 ---- 3 -- 4 ------ 5 --
type PassThrough struct {
	in  chan interface{}
	out chan interface{}
}

// NewPassThrough returns new PassThrough instance
func NewPassThrough() *PassThrough {
	passThrough := &PassThrough{
		make(chan interface{}),
		make(chan interface{}),
	}
	go passThrough.doStream()
	return passThrough
}

// Via streams data through given flow
func (pt *PassThrough) Via(flow streams.Flow) streams.Flow {
	go pt.transmit(flow)
	return flow
}

// To streams data to given sink
func (pt *PassThrough) To(sink streams.Sink) {
	pt.transmit(sink)
}

// Out returns channel for sending data
func (pt *PassThrough) Out() <-chan interface{} {
	return pt.out
}

// In returns channel for receiving data
func (pt *PassThrough) In() chan<- interface{} {
	return pt.in
}

func (pt *PassThrough) transmit(inlet streams.Inlet) {
	for elem := range pt.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (pt *PassThrough) doStream() {
	for elem := range pt.in {
		pt.out <- elem
	}
	close(pt.out)
}
