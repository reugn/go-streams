package extension

import (
	"fmt"

	"github.com/reugn/go-streams"
)

// StdoutSink represents a simple outbound connector that writes
// streaming data to standard output.
type StdoutSink struct {
	in   chan any
	done chan struct{}
}

var _ streams.Sink = (*StdoutSink)(nil)

// NewStdoutSink returns a new StdoutSink connector.
func NewStdoutSink() *StdoutSink {
	stdoutSink := &StdoutSink{
		in:   make(chan any),
		done: make(chan struct{}),
	}

	// asynchronously process stream data
	go stdoutSink.process()

	return stdoutSink
}

func (stdout *StdoutSink) process() {
	defer close(stdout.done)
	for elem := range stdout.in {
		fmt.Println(elem)
	}
}

// In returns the input channel of the StdoutSink connector.
func (stdout *StdoutSink) In() chan<- any {
	return stdout.in
}

// AwaitCompletion blocks until the StdoutSink has processed all received data.
func (stdout *StdoutSink) AwaitCompletion() {
	<-stdout.done
}

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
	for {
		_, ok := <-ignore.in
		if !ok {
			break
		}
	}
}

// In returns the input channel of the IgnoreSink connector.
func (ignore *IgnoreSink) In() chan<- any {
	return ignore.in
}

// AwaitCompletion is a no-op for the IgnoreSink.
func (ignore *IgnoreSink) AwaitCompletion() {
	// no-op
}
