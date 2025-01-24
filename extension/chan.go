package extension

import (
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ChanSource represents an inbound connector that creates a stream of
// elements from a channel.
type ChanSource struct {
	in chan any
}

var _ streams.Source = (*ChanSource)(nil)

// NewChanSource returns a new ChanSource connector.
func NewChanSource(in chan any) *ChanSource {
	return &ChanSource{in}
}

// Via asynchronously streams data to the given Flow and returns it.
func (cs *ChanSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(cs, operator)
	return operator
}

// Out returns the output channel of the ChanSource connector.
func (cs *ChanSource) Out() <-chan any {
	return cs.in
}

// ChanSink represents an outbound connector that writes streaming data
// to a channel.
type ChanSink struct {
	Out chan any
}

var _ streams.Sink = (*ChanSink)(nil)

// NewChanSink returns a new ChanSink connector.
func NewChanSink(out chan any) *ChanSink {
	return &ChanSink{out}
}

// In returns the input channel of the ChanSink connector.
func (ch *ChanSink) In() chan<- any {
	return ch.Out
}

// AwaitCompletion is a no-op for the ChanSink.
func (ch *ChanSink) AwaitCompletion() {
	// no-op
}
