package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func TestTumblingWindow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	slidingWindow := flow.NewTumblingWindow(50 * time.Millisecond)
	sink := ext.NewChanSink(out)

	go func() {
		inputValues := []string{"a", "b", "c", "d", "e", "f", "g"}
		for _, v := range inputValues {
			ingestDeferred(v, in, 15*time.Millisecond)
		}
		closeDeferred(in, 160*time.Millisecond)
	}()

	go func() {
		source.
			Via(slidingWindow).
			To(sink)
	}()

	var outputValues [][]interface{}
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]interface{}))
	}
	fmt.Println(outputValues)

	assertEquals(t, 3, len(outputValues)) // [[a b c] [d e f] [g]]

	assertEquals(t, []interface{}{"a", "b", "c"}, outputValues[0])
	assertEquals(t, []interface{}{"d", "e", "f"}, outputValues[1])
	assertEquals(t, []interface{}{"g"}, outputValues[2])
}
