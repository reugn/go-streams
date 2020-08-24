package streams

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

// A Sink is a set of stream processing steps that has one open input.
// Can be used as a Subscriber.
type Sink interface {
	Inlet
}
