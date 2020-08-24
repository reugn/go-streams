package flow

import (
	"sync"

	"github.com/reugn/go-streams"
)

// DoStream streams data from the outlet to inlet.
func DoStream(outlet streams.Outlet, inlet streams.Inlet) {
	go func() {
		for elem := range outlet.Out() {
			inlet.In() <- elem
		}

		close(inlet.In())
	}()
}

// Split splits the stream into two flows according to some criterion.
func Split(outlet streams.Outlet, cond func(interface{}) bool) [2]streams.Flow {
	condTrue := NewPassThrough()
	condFalse := NewPassThrough()

	go func() {
		for elem := range outlet.Out() {
			if cond(elem) {
				condTrue.In() <- elem
			} else {
				condFalse.In() <- elem
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
		for elem := range outlet.Out() {
			for _, socket := range out {
				socket.In() <- elem
			}
		}
		for i := 0; i < magnitude; i++ {
			close(out[i].In())
		}
	}()

	return out
}

// Merge merges multiple flows into a single flow.
func Merge(outlets ...streams.Flow) streams.Flow {
	merged := NewPassThrough()
	var wg sync.WaitGroup
	wg.Add(len(outlets))

	for _, out := range outlets {
		go func(outlet streams.Outlet) {
			for elem := range outlet.Out() {
				merged.In() <- elem
			}
			wg.Done()
		}(out)
	}

	// close merged.In() on the last outlet close.
	go func(wg *sync.WaitGroup) {
		wg.Wait()
		close(merged.In())
	}(&wg)

	return merged
}
