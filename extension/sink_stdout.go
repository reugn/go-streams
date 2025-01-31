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
