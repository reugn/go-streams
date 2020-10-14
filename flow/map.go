package flow

import (
	"github.com/reugn/go-streams"
)

// MapFunc is a Map transformation function.
type MapFunc func(interface{}) interface{}

// Map takes one element and produces one element.
//
// in  -- 1 -- 2 ---- 3 -- 4 ------ 5 --
//        |    |      |    |        |
//    [----------- MapFunc -------------]
//        |    |      |    |        |
// out -- 1' - 2' --- 3' - 4' ----- 5' -
type Map struct {
	MapF        MapFunc
	in          chan interface{}
	out         chan interface{}
	parallelism uint
}

// Verify Map satisfies the Flow interface.
var _ streams.Flow = (*Map)(nil)

// NewMap returns a new Map instance.
// mapFunc is the Map transformation function.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewMap(mapFunc MapFunc, parallelism uint) *Map {
	_map := &Map{
		mapFunc,
		make(chan interface{}),
		make(chan interface{}),
		parallelism,
	}
	go _map.doStream()
	return _map
}

// Via streams data through the given flow
func (m *Map) Via(flow streams.Flow) streams.Flow {
	go m.transmit(flow)
	return flow
}

// To streams data to the given sink
func (m *Map) To(sink streams.Sink) {
	m.transmit(sink)
}

// Out returns an output channel for sending data
func (m *Map) Out() <-chan interface{} {
	return m.out
}

// In returns an input channel for receiving data
func (m *Map) In() chan<- interface{} {
	return m.in
}

func (m *Map) transmit(inlet streams.Inlet) {
	for elem := range m.Out() {
		inlet.In() <- elem
	}
	close(inlet.In())
}

func (m *Map) doStream() {
	sem := make(chan struct{}, m.parallelism)
	for elem := range m.in {
		sem <- struct{}{}
		go func(e interface{}) {
			defer func() { <-sem }()
			trans := m.MapF(e)
			m.out <- trans
		}(elem)
	}
	for i := 0; i < int(m.parallelism); i++ {
		sem <- struct{}{}
	}
	close(m.out)
}
