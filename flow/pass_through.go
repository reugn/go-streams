package flow

import (
	"github.com/reugn/go-streams"
)

// PassThrough flow
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
// out -- 1 -- 2 ---- 3 -- 4 ------ 5 --
type PassThrough struct {
	in chan interface{}
}

// NewPassThrough returns new PassThrough instance
func NewPassThrough() *PassThrough {
	return &PassThrough{
		make(chan interface{}),
	}
}

// Via streams data through given flow
func (pt *PassThrough) Via(flow streams.Flow) streams.Flow {
	DoStream(pt, flow)
	return flow
}

// To streams data to given sink
func (pt *PassThrough) To(sink streams.Sink) {
	DoStream(pt, sink)
}

// Out returns channel for sending data
func (pt *PassThrough) Out() <-chan interface{} {
	return pt.in
}

// In returns channel for receiving data
func (pt *PassThrough) In() chan<- interface{} {
	return pt.in
}
