package streams

// Inlet represents a type that exposes one open input.
type Inlet interface {
	// In returns the input channel for the Inlet.
	// Data sent to this channel will be consumed by the component that implements
	// this interface. This channel should be closed by the upstream component
	// when no more input is expected.
	In() chan<- any
}

// Outlet represents a type that exposes one open output.
type Outlet interface {
	// Out returns the output channel for the Outlet.
	// Data sent to this channel can be consumed by another component further
	// in the processing pipeline. This channel should be closed by the implementing
	// component when upstream processing has been completed.
	Out() <-chan any
}

// Source represents a set of stream processing steps that has one open output.
// A Source will usually connect to a database or streaming platform to produce
// a stream of events/records.
// Implement this interface to create a custom source connector.
type Source interface {
	Outlet
	// Via asynchronously streams data from the Source's Outlet to the given Flow.
	// It should return a new Flow that represents the combined pipeline.
	Via(Flow) Flow
}

// Flow represents a set of stream processing steps that has one open input
// and one open output.
// Programs can combine multiple Flows into sophisticated dataflow topologies.
// Implement this interface to create a custom stream transformation operator.
type Flow interface {
	Inlet
	Outlet
	// Via asynchronously streams data from the Flow's Outlet to the given Flow.
	// It should return a new Flow that represents the combined pipeline.
	Via(Flow) Flow
	// To streams data from the Flow's Outlet to the given Sink, and should block
	// until the Sink has completed processing all data, which can be verified
	// via the Sink's AwaitCompletion method.
	To(Sink)
}

// Sink represents a set of stream processing steps that has one open input.
// A Sink will usually connect to a database or streaming platform to flush
// data from a pipeline.
// Implement this interface to create a custom sink connector.
type Sink interface {
	Inlet
	// AwaitCompletion should block until the Sink has completed processing
	// all data received through its Inlet and has finished any necessary
	// finalization or cleanup tasks.
	// This method is intended for internal use by the pipeline when the
	// input stream is closed by the upstream.
	AwaitCompletion()
}
