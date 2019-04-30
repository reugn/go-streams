package streams

//In port interface
type Inlet interface {
	In() chan<- interface{}
}

//Out port interface
type Outlet interface {
	Out() <-chan interface{}
}

//Source adaptor
type Source interface {
	Outlet
	Via(Flow) Flow
}

//Stream transformation interface
type Flow interface {
	Inlet
	Outlet
	Via(Flow) Flow
	To(Sink)
}

//Sink adaptor
type Sink interface {
	Inlet
}
