package flow

import (
	"github.com/reugn/go-streams"
)

// FlatMapFunc transformer
type FlatMapFunc func(interface{}) []interface{}

// FlatMap function transformation flow
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
//    [---------- FlatMapFunc ----------]
//        |    |           |   |    |
// out -- 1' - 2' -------- 4'- 4''- 5' -
type FlatMap struct {
	FlatMapF    FlatMapFunc
	in          chan interface{}
	out         chan interface{}
	parallelism uint
}

// NewFlatMap returns new FlatMap instance
// FlatMapFunc - transformation function
// parallelism - parallelism factor, in case events order matters use parallelism = 1
func NewFlatMap(f FlatMapFunc, parallelism uint) *FlatMap {
	flatMap := &FlatMap{
		f,
		make(chan interface{}),
		make(chan interface{}),
		parallelism,
	}
	go flatMap.doStream()
	return flatMap
}

// Via streams data through given flow
func (fm *FlatMap) Via(flow streams.Flow) streams.Flow {
	go fm.transmit(flow)
	return flow
}

// To streams data to given sink
func (fm *FlatMap) To(sink streams.Sink) {
	fm.transmit(sink)
}

// Out returns channel for sending data
func (fm *FlatMap) Out() <-chan interface{} {
	return fm.out
}

// In returns channel for receiving data
func (fm *FlatMap) In() chan<- interface{} {
	return fm.in
}

func (fm *FlatMap) transmit(inlet streams.Inlet) {
	for elem := range fm.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (fm *FlatMap) doStream() {
	sem := make(chan struct{}, fm.parallelism)
	for elem := range fm.in {
		sem <- struct{}{}
		go func(e interface{}) {
			defer func() { <-sem }()
			trans := fm.FlatMapF(e)
			for _, item := range trans {
				fm.out <- item
			}
		}(elem)
	}
	for i := 0; i < int(fm.parallelism); i++ {
		sem <- struct{}{}
	}
	close(fm.out)
}
