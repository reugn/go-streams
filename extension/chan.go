package ext

import (
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type ChanSource struct {
	in chan interface{}
}

func NewChanSource(in chan interface{}) *ChanSource {
	return &ChanSource{in}
}

func (cs *ChanSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(cs, _flow)
	return _flow
}

func (cs *ChanSource) Out() <-chan interface{} {
	return cs.in
}

type ChanSink struct {
	Out chan interface{}
}

func NewChanSink(out chan interface{}) *ChanSink {
	return &ChanSink{out}
}

func (ch *ChanSink) In() chan<- interface{} {
	return ch.Out
}
