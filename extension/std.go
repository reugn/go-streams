package extension

import "fmt"

// StdoutSink represents a simple outbound connector that sends incoming
// items to standard output.
type StdoutSink struct {
	in chan interface{}
}

// NewStdoutSink returns a new StdoutSink instance.
func NewStdoutSink() *StdoutSink {
	sink := &StdoutSink{
		in: make(chan interface{}),
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

// In returns an input channel for receiving data
func (stdout *StdoutSink) In() chan<- interface{} {
	return stdout.in
}

// IgnoreSink represents a simple outbound connector that discards
// all of the incoming items.
type IgnoreSink struct {
	in chan interface{}
}

// NewIgnoreSink returns a new IgnoreSink instance.
func NewIgnoreSink() *IgnoreSink {
	sink := &IgnoreSink{
		in: make(chan interface{}),
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

// In returns an input channel for receiving data
func (ignore *IgnoreSink) In() chan<- interface{} {
	return ignore.in
}
