package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func TestTumblingWindow(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	tumblingWindow := flow.NewTumblingWindow[string](50 * time.Millisecond)
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
			Via(tumblingWindow).
			Via(flow.NewMap(retransmitStringSlice, 1)). // test generic return type
			To(sink)
	}()

	var outputValues [][]string
	for e := range sink.Out {
		outputValues = append(outputValues, e.([]string))
	}
	fmt.Println(outputValues)

	assertEquals(t, 3, len(outputValues)) // [[a b c] [d e f] [g]]

	assertEquals(t, []string{"a", "b", "c"}, outputValues[0])
	assertEquals(t, []string{"d", "e", "f"}, outputValues[1])
	assertEquals(t, []string{"g"}, outputValues[2])
}
