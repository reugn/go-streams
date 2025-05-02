package flow

import (
	"sync"

	"github.com/reugn/go-streams"
)

// Keyed represents a flow where stream elements are partitioned by key
// using a provided key selector function.
type Keyed[K comparable, V any] struct {
	keySelector func(V) K
	keyedFlows  map[K]streams.Flow
	operators   []func() streams.Flow

	in  chan any
	out chan any
}

// Verify Keyed satisfies the Flow interface.
var _ streams.Flow = (*Keyed[int, any])(nil)

// NewKeyed returns a new Keyed operator. This operator splits an input stream
// into multiple sub-streams based on keys extracted from the elements using
// the keySelector function.
//
// Each of these individual streams is then transformed by the provided chain
// of operators, and the results are sent to the output channel. Due to the
// concurrent processing of each keyed stream, the order of elements in the
// output channel is not deterministic and may not reflect the original order
// of elements in the input stream.
//
// Each operator supplier must return a new instance of the flow to ensure
// that each keyed stream has its own independent state.
//
// Example:
//
//	newSlidingWindow := func() streams.Flow {
//		return flow.NewSlidingWindow[event](10*time.Second, time.Second)
//	}
//
//	newMap := func() streams.Flow {
//		return flow.NewMap(func(events []event) event {
//			return events[len(events)-1]
//		}, 1)
//	}
//
//	keyed := flow.NewKeyed(func(e event) string {
//		return e.serial
//	}, newSlidingWindow, newMap)
//
// If no operators are provided, NewKeyed will panic.
func NewKeyed[K comparable, V any](
	keySelector func(V) K, operators ...func() streams.Flow,
) *Keyed[K, V] {
	if len(operators) == 0 {
		panic("at least one operator supplier is required")
	}

	keyedFlow := &Keyed[K, V]{
		keySelector: keySelector,
		keyedFlows:  make(map[K]streams.Flow),
		operators:   operators,
		in:          make(chan any),
		out:         make(chan any),
	}

	// start stream processing
	go keyedFlow.stream()

	return keyedFlow
}

// stream routes incoming elements to keyed workflows and consolidates
// the results into the output channel of the Keyed flow.
func (k *Keyed[K, V]) stream() {
	var wg sync.WaitGroup
	for element := range k.in {
		// extract element's key using the selector
		key := k.keySelector(element.(V))
		// retrieve the keyed flow for the key
		keyedFlow := k.getKeyedFlow(key, &wg)
		// send the element downstream
		keyedFlow.In() <- element
	}

	// close all keyed streams
	for _, keyedFlow := range k.keyedFlows {
		close(keyedFlow.In())
	}

	// wait for all keyed streams to complete
	wg.Wait()
	close(k.out)
}

// Via asynchronously streams data to the given Flow and returns it.
func (k *Keyed[K, V]) Via(flow streams.Flow) streams.Flow {
	go k.transmit(flow)
	return flow
}

// To streams data to the given Sink and blocks until the Sink has completed
// processing all data.
func (k *Keyed[K, V]) To(sink streams.Sink) {
	k.transmit(sink)
	sink.AwaitCompletion()
}

// Out returns the output channel of the Keyed operator.
func (k *Keyed[K, V]) Out() <-chan any {
	return k.out
}

// In returns the input channel of the Keyed operator.
func (k *Keyed[K, V]) In() chan<- any {
	return k.in
}

// transmit submits keyed elements to the next Inlet.
func (k *Keyed[K, V]) transmit(inlet streams.Inlet) {
	for keyed := range k.out {
		inlet.In() <- keyed
	}
	close(inlet.In())
}

// getKeyedFlow retrieves a keyed workflow associated with the provided key.
// If the workflow has not yet been initiated, it will be created and a
// goroutine will be launched to handle the stream.
func (k *Keyed[K, V]) getKeyedFlow(key K, wg *sync.WaitGroup) streams.Flow {
	// try to retrieve the keyed flow from the map
	keyedWorkflow, ok := k.keyedFlows[key]
	if !ok { // this is the first element for the key
		wg.Add(1)

		// build the workflow
		keyedWorkflow = k.operators[0]()
		workflowTail := keyedWorkflow
		for _, operatorFactory := range k.operators[1:] {
			workflowTail = workflowTail.Via(operatorFactory())
		}

		// start a goroutine to forward processed elements from the
		// keyed workflow to the output channel
		go func() {
			defer wg.Done()
			for e := range workflowTail.Out() {
				k.out <- e
			}
		}()

		// associate the key with the workflow
		k.keyedFlows[key] = keyedWorkflow
	}

	return keyedWorkflow
}
