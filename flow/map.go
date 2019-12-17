package flow

import (
	"github.com/reugn/go-streams"
)

// MapFunc transformer
type MapFunc func(interface{}) interface{}

// Map function transformation flow
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

// NewMap returns new Map instance
// MapFunc - transformation function
// parallelism - parallelism factor, in case events order matters use parallelism = 1
func NewMap(f MapFunc, parallelism uint) *Map {
	_map := &Map{
		f,
		make(chan interface{}),
		make(chan interface{}),
		parallelism,
	}
	go _map.doStream()
	return _map
}

// Via streams data through given flow
func (m *Map) Via(flow streams.Flow) streams.Flow {
	go m.transmit(flow)
	return flow
}

// To streams data to given sink
func (m *Map) To(sink streams.Sink) {
	m.transmit(sink)
}

// Out returns channel for sending data
func (m *Map) Out() <-chan interface{} {
	return m.out
}

// In returns channel for receiving data
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
