package extension

import (
	"fmt"

	"github.com/reugn/go-streams"
)

// StdoutSink represents a simple outbound connector that writes
// streaming data to standard output.
type StdoutSink struct {
	in chan any
}

var _ streams.Sink = (*StdoutSink)(nil)

// NewStdoutSink returns a new StdoutSink connector.
func NewStdoutSink() *StdoutSink {
	sink := &StdoutSink{
		in: make(chan any),
	}
	sink.init()

	return sink
}

func (stdout *StdoutSink) init() {
	go func() {
		for elem := range stdout.in {
			fmt.Println(elem)
		}
	}()
}

// In returns the input channel of the StdoutSink connector.
func (stdout *StdoutSink) In() chan<- any {
	return stdout.in
}

// IgnoreSink represents a simple outbound connector that discards
// all elements of a stream.
type IgnoreSink struct {
	in chan any
}

var _ streams.Sink = (*IgnoreSink)(nil)

// NewIgnoreSink returns a new IgnoreSink connector.
func NewIgnoreSink() *IgnoreSink {
	sink := &IgnoreSink{
		in: make(chan any),
	}
	sink.init()

	return sink
}

func (ignore *IgnoreSink) init() {
	go func() {
		for {
			_, ok := <-ignore.in
			if !ok {
				break
			}
		}
	}()
}

// In returns the input channel of the IgnoreSink connector.
func (ignore *IgnoreSink) In() chan<- any {
	return ignore.in
}
