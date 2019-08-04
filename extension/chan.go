package ext

import (
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ChanSource streams data from chan
type ChanSource struct {
	in chan interface{}
}

// NewChanSource returns new ChanSource instance
func NewChanSource(in chan interface{}) *ChanSource {
	return &ChanSource{in}
}

// Via streams data through given flow
func (cs *ChanSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(cs, _flow)
	return _flow
}

// Out returns channel for sending data
func (cs *ChanSource) Out() <-chan interface{} {
	return cs.in
}

// ChanSink transmitts data to chan
type ChanSink struct {
	Out chan interface{}
}

// NewChanSink returns new ChanSink instance
func NewChanSink(out chan interface{}) *ChanSink {
	return &ChanSink{out}
}

// In returns channel for receiving data
func (ch *ChanSink) In() chan<- interface{} {
	return ch.Out
}
