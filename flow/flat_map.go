package flow

import (
	"github.com/reugn/go-streams"
)

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
	parallelism uint
}

// NewFlatMap
// FlatMapFunc - transformation function
// parallelism - parallelism factor, in case events order matters use parallelism = 1
func NewFlatMap(f FlatMapFunc, parallelism uint) *FlatMap {
	return &FlatMap{
		f,
		make(chan interface{}),
		parallelism,
	}
}

func (fm *FlatMap) Via(flow streams.Flow) streams.Flow {
	go fm.doStream(flow)
	return flow
}

func (fm *FlatMap) To(sink streams.Sink) {
	fm.doStream(sink)
}

func (fm *FlatMap) Out() <-chan interface{} {
	return fm.in
}

func (fm *FlatMap) In() chan<- interface{} {
	return fm.in
}

func (fm *FlatMap) doStream(inlet streams.Inlet) {
	sem := make(chan struct{}, fm.parallelism)
	for elem := range fm.in {
		sem <- struct{}{}
		go func(e interface{}) {
			defer func() { <-sem }()
			trans := fm.FlatMapF(e)
			for _, item := range trans {
				inlet.In() <- item
			}
		}(elem)
	}
	for i := 0; i < int(fm.parallelism); i++ {
		sem <- struct{}{}
	}
	close(inlet.In())
}
