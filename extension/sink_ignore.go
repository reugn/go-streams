package extension

import "github.com/reugn/go-streams"

// IgnoreSink represents a simple outbound connector that discards
// all elements of a stream.
type IgnoreSink struct {
	in chan any
}

var _ streams.Sink = (*IgnoreSink)(nil)

// NewIgnoreSink returns a new IgnoreSink connector.
func NewIgnoreSink() *IgnoreSink {
	ignoreSink := &IgnoreSink{
		in: make(chan any),
	}

	// asynchronously process stream data
	go ignoreSink.process()

	return ignoreSink
}

func (ignore *IgnoreSink) process() {
	drainChan(ignore.in)
}

// In returns the input channel of the IgnoreSink connector.
func (ignore *IgnoreSink) In() chan<- any {
	return ignore.in
}

// AwaitCompletion is a no-op for the IgnoreSink.
func (ignore *IgnoreSink) AwaitCompletion() {
	// no-op
}
