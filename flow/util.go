package flow

import (
	"fmt"
	"sync"

	"github.com/reugn/go-streams"
)

// DoStream streams data from the outlet to inlet.
func DoStream(outlet streams.Outlet, inlet streams.Inlet) {
	go func() {
		for element := range outlet.Out() {
			inlet.In() <- element
		}

		close(inlet.In())
	}()
}

// Split splits the stream into two flows according to the given boolean predicate.
// T specifies the incoming and outgoing element type.
func Split[T any](outlet streams.Outlet, predicate func(T) bool) [2]streams.Flow {
	condTrue := NewPassThrough()
	condFalse := NewPassThrough()

	go func() {
		for element := range outlet.Out() {
			if predicate(element.(T)) {
				condTrue.In() <- element
			} else {
				condFalse.In() <- element
			}
		}

		close(condTrue.In())
		close(condFalse.In())
	}()

	return [...]streams.Flow{condTrue, condFalse}
}

// FanOut creates a number of identical flows from the single outlet.
// This can be useful when writing to multiple sinks is required.
func FanOut(outlet streams.Outlet, magnitude int) []streams.Flow {
	out := make([]streams.Flow, magnitude)
	for i := 0; i < magnitude; i++ {
		out[i] = NewPassThrough()
	}

	go func() {
		for element := range outlet.Out() {
			for _, flow := range out {
				flow.In() <- element
			}
		}
		for _, flow := range out {
			close(flow.In())
		}
	}()

	return out
}

// RoundRobin creates a balanced number of flows from the single outlet.
// This can be useful when work can be parallelized across multiple cores.
func RoundRobin(outlet streams.Outlet, magnitude int) []streams.Flow {
	out := make([]streams.Flow, magnitude)
	for i := 0; i < magnitude; i++ {
		out[i] = NewPassThrough()
		go func(o streams.Flow) {
			defer close(o.In())
			for element := range outlet.Out() {
				o.In() <- element
			}
		}(out[i])
	}

	return out
}

// Merge merges multiple flows into a single flow.
// When all specified outlets are closed, the resulting flow will close.
func Merge(outlets ...streams.Flow) streams.Flow {
	merged := NewPassThrough()
	var wg sync.WaitGroup
	wg.Add(len(outlets))

	for _, out := range outlets {
		go func(outlet streams.Outlet) {
			for element := range outlet.Out() {
				merged.In() <- element
			}
			wg.Done()
		}(out)
	}

	// close the in channel on the last outlet close.
	go func() {
		wg.Wait()
		close(merged.In())
	}()

	return merged
}

// ZipWith combines elements from multiple input streams using a combiner function.
// It returns a new Flow with the resulting values. The combiner function is called
// with a slice of elements, where each element is taken from each input outlet.
// The elements in the slice will be in the order of outlets. If an outlet
// is closed, its corresponding element in the slice will be the zero value.
// The returned Flow will close when all the input outlets are closed.
//
// It will panic if provided less than two outlets, or if any of the outlets has an
// element type other than T.
func ZipWith[T, R any](combine func([]T) R, outlets ...streams.Outlet) streams.Flow {
	// validate outlets length
	if len(outlets) < 2 {
		panic(fmt.Errorf("outlets length %d must be at least 2", len(outlets)))
	}

	combined := NewPassThrough()
	// asynchronously populate the flow with zipped elements
	go func() {
		var zero T
		head := outlets[0]
		tail := outlets[1:]
		for n := 0; n < len(outlets); n++ {
			for element := range head.Out() {
				// initialize the slice to contain one element per outlet
				zipped := make([]T, len(outlets))
				// fill zero elements for the closed outlets
				for j := 0; j < n; j++ {
					zipped[j] = zero
				}
				// set the value from the head outlet
				zipped[n] = element.(T)
				// read elements from subsequent outlets
				for i, outlet := range tail {
					// this read will block until an element is available
					// for the outlet, or it is closed by the upstream
					e, ok := <-outlet.Out()
					if ok {
						zipped[i+n+1] = e.(T)
					} else {
						zipped[i+n+1] = zero
					}
				}
				// send the result of the combiner function downstream;
				// at this point zipped has at least one non-empty value
				// taken from the current head
				combined.In() <- combine(zipped)
			}
			// when the head channel is closed move the head to the next
			// outlet and advance the tail slice
			switch n {
			case len(outlets) - 1:
				// last iteration
			case len(outlets) - 2:
				head = outlets[n+1]
				tail = nil
			default:
				head = outlets[n+1]
				tail = outlets[n+2:]
			}
		}
		close(combined.In()) // all provided outlets are closed
	}()

	return combined
}

// Flatten creates a Flow to flatten the stream of slices.
// T specifies the outgoing element type, and the incoming element type is []T.
func Flatten[T any](parallelism int) streams.Flow {
	return NewFlatMap(func(element []T) []T {
		return element
	}, parallelism)
}
