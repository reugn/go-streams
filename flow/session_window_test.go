package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func TestSessionWindow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	sessionWindow := flow.NewSessionWindow(20 * time.Millisecond)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c"}
	go ingestSlice(inputValues, in)
	go ingestDeferred("d", in, 30*time.Millisecond)
	go ingestDeferred("e", in, 70*time.Millisecond)
	go closeDeferred(in, 100*time.Millisecond)

	go func() {
		source.
			Via(sessionWindow).
			To(sink)
	}()

	var outputValues [][]interface{}
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]interface{}))
	}
	fmt.Println(outputValues)

	assertEquals(t, 3, len(outputValues)) // [[a b c] [d] [e]]

	assertEquals(t, []interface{}{"a", "b", "c"}, outputValues[0])
	assertEquals(t, []interface{}{"d"}, outputValues[1])
	assertEquals(t, []interface{}{"e"}, outputValues[2])
}

func TestLongSessionWindow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	sessionWindow := flow.NewSessionWindow(20 * time.Millisecond)
	sink := ext.NewChanSink(out)

	inputValues := []string{"a", "b", "c", "d", "e", "f", "g"}
	go func() {
		for _, e := range inputValues {
			ingestDeferred(e, in, 10*time.Millisecond)
		}
	}()
	go ingestDeferred("h", in, 140*time.Millisecond)
	go closeDeferred(in, 150*time.Millisecond)

	go func() {
		source.
			Via(sessionWindow).
			To(sink)
	}()

	var outputValues [][]interface{}
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]interface{}))
	}
	fmt.Println(outputValues)

	assertEquals(t, 2, len(outputValues)) // [[a b c d e f g] [h]]

	assertEquals(t, []interface{}{"a", "b", "c", "d", "e", "f", "g"}, outputValues[0])
	assertEquals(t, []interface{}{"h"}, outputValues[1])
}
