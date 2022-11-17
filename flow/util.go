package flow

import (
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
func Split(outlet streams.Outlet, predicate func(interface{}) bool) [2]streams.Flow {
	condTrue := NewPassThrough()
	condFalse := NewPassThrough()

	go func() {
		for element := range outlet.Out() {
			if predicate(element) {
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
	var out []streams.Flow
	for i := 0; i < magnitude; i++ {
		out = append(out, NewPassThrough())
	}

	go func() {
		for element := range outlet.Out() {
			for _, socket := range out {
				socket.In() <- element
			}
		}
		for i := 0; i < magnitude; i++ {
			close(out[i].In())
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
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(merged.In())
	}(&wg)

	return merged
}

// Flatten creates a Flow to flatten the stream of slices.
func Flatten(parallelism uint) streams.Flow {
	return NewFlatMap(func(element []any) []any {
		return element
	}, parallelism)
}

/*
ZipWith takes an element from each outlet and zips them into the user-defined combineFunc() result.

The order of items in "elements" param of combineFunc os the same order of the given outlets.

For example, given sources s1 and s2 emmitting "a" and "b" respectively, combineFunc will be called with elements = *[]string{"a", "b"}.

ZipWith blocks until all each outlet emmitted a value before emmiting to its output channel.
*/
func ZipWith[T, Z any](combineFunc func(elements *[]T) Z, outlets ...streams.Outlet) streams.Flow {

	first := outlets[0]
	rest := outlets[1:]
	outc := NewPassThrough()

	go func() {
		for element := range first.Out() {
			zipped := make([]T, len(outlets))
			zipped[0] = element.(T)
			for i, outlet := range rest {
				e := <-outlet.Out()
				zipped[i+1] = e.(T)
			}
			outc.In() <- combineFunc(&zipped)
		}
		close(outc.In())
	}()

	return outc
}
