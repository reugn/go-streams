package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func TestBatch(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	batch := flow.NewBatch[string](4, 40*time.Millisecond)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c", "d", "e", "f", "g"}
	go func() {
		for _, e := range inputValues {
			ingestDeferred(e, in, 5*time.Millisecond)
		}
	}()
	go ingestDeferred("h", in, 90*time.Millisecond)
	go closeDeferred(in, 100*time.Millisecond)

	go func() {
		source.
			Via(batch).
			Via(flow.NewMap(retransmitStringSlice, 1)). // test generic return type
			To(sink)
	}()

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]string))
	}
	fmt.Println(outputValues)

	assertEquals(t, 3, len(outputValues)) // [[a b c d] [e f g] [h]]

	assertEquals(t, []string{"a", "b", "c", "d"}, outputValues[0])
	assertEquals(t, []string{"e", "f", "g"}, outputValues[1])
	assertEquals(t, []string{"h"}, outputValues[2])
}
