package flow

import (
	"github.com/reugn/go-streams"
)

// Pass Through flow
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
// out -- 1 -- 2 ---- 3 -- 4 ------ 5 --
type PassThrough struct {
	in chan interface{}
}

func NewPassThrough() *PassThrough {
	return &PassThrough{
		make(chan interface{}),
	}
}

func (pt *PassThrough) Via(flow streams.Flow) streams.Flow {
	DoStream(pt, flow)
	return flow
}

func (pt *PassThrough) To(sink streams.Sink) {
	DoStream(pt, sink)
}

func (pt *PassThrough) Out() <-chan interface{} {
	return pt.in
}

func (pt *PassThrough) In() chan<- interface{} {
	return pt.in
}
