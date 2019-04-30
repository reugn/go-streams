package ext

import "fmt"

type StdoutSink struct {
	in chan interface{}
}

func NewStdoutSink() *StdoutSink {
	sink := &StdoutSink{make(chan interface{})}
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

func (stdout *StdoutSink) In() chan<- interface{} {
	return stdout.in
}

// /dev/null Sink
type IgnoreSink struct {
	in chan interface{}
}

func NewIgnoreSink() *IgnoreSink {
	sink := &IgnoreSink{make(chan interface{})}
	sink.init()
	return sink
}

func (ignore *IgnoreSink) init() {
	go func() {
		for {
			_, ok := (<-ignore.in)
			if !ok {
				break
			}
		}
	}()
}

func (ignore *IgnoreSink) In() chan<- interface{} {
	return ignore.in
}
