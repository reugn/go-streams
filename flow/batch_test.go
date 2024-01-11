package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func TestBatch(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	batch := flow.NewBatch(4, 40*time.Millisecond)
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
			To(sink)
	}()

	var outputValues [][]interface{}
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]interface{}))
	}
	fmt.Println(outputValues)

	assertEquals(t, 3, len(outputValues)) // [[a b c d] [e f g] [h]]

	assertEquals(t, []interface{}{"a", "b", "c", "d"}, outputValues[0])
	assertEquals(t, []interface{}{"e", "f", "g"}, outputValues[1])
	assertEquals(t, []interface{}{"h"}, outputValues[2])
}
