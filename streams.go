package streams

// Inlet represents a type that exposes one open input.
type Inlet interface {
	In() chan<- interface{}
}

// Outlet represents a type that exposes one open output.
type Outlet interface {
	Out() <-chan interface{}
}

// Source represents a set of stream processing steps that has one open output.
type Source interface {
	Outlet
	Via(Flow) Flow
}

// Flow represents a set of stream processing steps that has one open input and one open output.
type Flow interface {
	Inlet
	Outlet
	Via(Flow) Flow
	To(Sink)
}

// Sink represents a set of stream processing steps that has one open input.
type Sink interface {
	Inlet
}
