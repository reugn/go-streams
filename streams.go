package streams

import (
	"sync"
)

// Inlet is a type that exposes one open input.
// Implemented by the Flow and Sink.
type Inlet interface {
	In() chan<- interface{}
}

// Outlet is a type that exposes one open output.
// Implemented by the Source and Flow.
type Outlet interface {
	Out() <-chan interface{}
}

// A Source is a set of stream processing steps that has one open output.
type Source interface {
	Outlet
	Via(Flow) Flow
}

// A Flow is a set of stream processing steps that has one open input and one open output.
type Flow interface {
	Inlet
	Outlet
	Via(Flow) Flow
	To(Sink)
}

type SingleFlow struct {
	Flow
}

func (f SingleFlow) Print() {
	go func() {
		for i := range f.Out() {
			println(i)
		}
		close(f.In())
	}()
}

// KeyedData key and data
type KeyedData struct {
	Key  interface{}
	Data interface{}
}

// KeyedFlow KeyedData han
type KeyedFlow struct {
	Ch chan KeyedData
}

// Connect Convert KeyedFlow to ConnectedFlow
// Merge two keyedFlow into ConnectedFlow
func (f1 *KeyedFlow) Connect(
	f2 *KeyedFlow,
	parallelism uint,
) *ConnectedFlow {
	result := ConnectedFlow{}
	result.Ch = make(chan ConnectData, parallelism)

	go func() {
		for i := range f1.Ch {
			result.Ch <- ConnectData{
				Key:    i.Key,
				Data:   i.Data,
				IsLeft: true,
			}
		}
	}()
	go func() {
		for i := range f2.Ch {
			result.Ch <- ConnectData{
				Key:    i.Key,
				Data:   i.Data,
				IsLeft: false,
			}
		}
	}()

	return &result
}

// ConnectedFlow ConnectData Chan
type ConnectedFlow struct {
	Ch chan ConnectData
}

// ConnectData key and data
// When kf1.Connect(kf2), the data of kf1 IsLeft=true, the data of kf1 IsLeft=false
type ConnectData struct {
	Key    interface{}
	Data   interface{}
	IsLeft bool
}

// CoFlatMap convert ConnectedFlow to Flow
// Apply fn function to  generate new data
func (cf *ConnectedFlow) CoFlatMap(
	fn func(v1 interface{}, v2 interface{}, out chan<- interface{}),
	parallelism uint,
) chan interface{} {
	result := make(chan interface{}, parallelism)
	m := sync.Map{}

	go func() {
		for item := range cf.Ch {
			data := item.Data
			key := item.Key
			values, _ := m.Load(key)
			var arr [2]interface{}
			if values == nil {
				arr = [2]interface{}{}
			} else {
				arr = values.([2]interface{})
			}
			if item.IsLeft {
				arr[0] = data
			} else {
				arr[1] = data
			}
			m.Store(key, arr)
			fn(arr[0], arr[1], result)
		}
		close(result)
	}()

	return result
}

// A Sink is a set of stream processing steps that has one open input.
// Can be used as a Subscriber.
type Sink interface {
	Inlet
}
