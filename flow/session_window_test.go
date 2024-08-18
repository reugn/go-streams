package flow_test

import (
	"fmt"
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

func TestSessionWindow(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	sessionWindow := flow.NewSessionWindow[string](20 * time.Millisecond)
	sink := ext.NewChanSink(out)
	assert.NotEqual(t, sessionWindow.Out(), nil)

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

	outputValues := readSlice[[]string](sink.Out)
	fmt.Println(outputValues)

	assert.Equal(t, 3, len(outputValues)) // [[a b c] [d] [e]]

	assert.Equal(t, []string{"a", "b", "c"}, outputValues[0])
	assert.Equal(t, []string{"d"}, outputValues[1])
	assert.Equal(t, []string{"e"}, outputValues[2])
}

func TestSessionWindow_Long(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	sessionWindow := flow.NewSessionWindow[string](20 * time.Millisecond)
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

	outputValues := readSlice[[]string](sink.Out)
	fmt.Println(outputValues)

	assert.Equal(t, 2, len(outputValues)) // [[a b c d e f g] [h]]

	assert.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g"}, outputValues[0])
	assert.Equal(t, []string{"h"}, outputValues[1])
}

func TestSessionWindow_Ptr(t *testing.T) {
	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	sessionWindow := flow.NewSessionWindow[*string](20 * time.Millisecond)
	sink := ext.NewChanSink(out)
	assert.NotEqual(t, sessionWindow.Out(), nil)

	inputValues := ptrSlice([]string{"a", "b", "c"})
	go ingestSlice(inputValues, in)
	go ingestDeferred(ptr("d"), in, 30*time.Millisecond)
	go ingestDeferred(ptr("e"), in, 70*time.Millisecond)
	go closeDeferred(in, 100*time.Millisecond)

	go func() {
		source.
			Via(sessionWindow).
			Via(flow.NewPassThrough()). // Via coverage
			To(sink)
	}()

	outputValues := readSlice[[]*string](sink.Out)
	fmt.Println(outputValues)

	assert.Equal(t, 3, len(outputValues)) // [[a b c] [d] [e]]

	assert.Equal(t, ptrSlice([]string{"a", "b", "c"}), outputValues[0])
	assert.Equal(t, ptrSlice([]string{"d"}), outputValues[1])
	assert.Equal(t, ptrSlice([]string{"e"}), outputValues[2])
}
