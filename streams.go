package streams

// Inlet represents a type that exposes one open input.
type Inlet interface {
	In() chan<- any
}

// Outlet represents a type that exposes one open output.
type Outlet interface {
	Out() <-chan any
}

// Source represents a set of stream processing steps that has one open output.
// A Source will usually connect to a database or streaming platform to produce
// a stream of events/records.
// Implement this interface to create a custom source connector.
type Source interface {
	Outlet
	Via(Flow) Flow
}

// Flow represents a set of stream processing steps that has one open input
// and one open output.
// Programs can combine multiple Flows into sophisticated dataflow topologies.
// Implement this interface to create a custom stream transformation operator.
type Flow interface {
	Inlet
	Outlet
	Via(Flow) Flow
	To(Sink)
}

// Sink represents a set of stream processing steps that has one open input.
// A Sink will usually connect to a database or streaming platform to flush
// data from a pipeline.
// Implement this interface to create a custom sink connector.
type Sink interface {
	Inlet
}
