package flow_test

import (
	"testing"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func TestBatchCase1(t *testing.T) {
	// only max size is reached

	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	batch := flow.NewBatch(2, time.Millisecond*100)

	inputValues := []string{"a", "b", "c", "d", "e"}
	go ingestSlice(inputValues, in)

	go closeDeferred(in, time.Millisecond*200)

	go func() {
		source.
			Via(batch).
			To(ext.NewChanSink(out))
	}()

	gottenValues := make([][]any, 0)
	for e := range out {
		gottenValues = append(gottenValues, e.([]any))
	}

	assertEquals(t, 3, len(gottenValues))

	assertEquals(t, []any{"a", "b"}, gottenValues[0])
	assertEquals(t, []any{"c", "d"}, gottenValues[1])
	assertEquals(t, []any{"e"}, gottenValues[2])
}

func TestBatchCase2(t *testing.T) {
	// only max time is reached

	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	batch := flow.NewBatch(3, time.Millisecond*10)

	inputValues := []string{"a", "b", "c", "d", "e"}
	for i, v := range inputValues {
		go ingestDeferred(v, in, time.Millisecond*time.Duration((i+1)*20))
	}

	go closeDeferred(in, time.Millisecond*200)

	go func() {
		source.
			Via(batch).
			To(ext.NewChanSink(out))
	}()

	gottenValues := make([][]any, 0)
	for e := range out {
		gottenValues = append(gottenValues, e.([]any))
	}

	assertEquals(t, 5, len(gottenValues))

	assertEquals(t, []any{"a"}, gottenValues[0])
	assertEquals(t, []any{"b"}, gottenValues[1])
	assertEquals(t, []any{"c"}, gottenValues[2])
	assertEquals(t, []any{"d"}, gottenValues[3])
	assertEquals(t, []any{"e"}, gottenValues[4])
}

func TestBatchCase3(t *testing.T) {
	// both max size and max time are reached.
	// it's the example in the Batch documentation

	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	batch := flow.NewBatch(3, time.Millisecond*100)

	go ingestDeferred("a", in, time.Millisecond*10)
	go ingestDeferred("b", in, time.Millisecond*15)
	go ingestDeferred("c", in, time.Millisecond*20)
	// batch1: [a b c] (max size)

	// ticker reset at time 20ms

	go ingestDeferred("d", in, time.Millisecond*40)
	go ingestDeferred("e", in, time.Millisecond*110)

	// batch2: [d e] (max time)

	// ticker reached timeout at 120ms and reset at 120ms

	// between 120ms and 220ms, there is no element, so there is no batch

	go ingestDeferred("f", in, time.Millisecond*300)

	// batch3: [f] (max time)

	go closeDeferred(in, time.Millisecond*330)

	go func() {
		source.
			Via(batch).
			To(ext.NewChanSink(out))
	}()

	gottenValues := make([][]any, 0)
	for e := range out {
		gottenValues = append(gottenValues, e.([]any))
	}

	assertEquals(t, 3, len(gottenValues))

	assertEquals(t, []any{"a", "b", "c"}, gottenValues[0])
	assertEquals(t, []any{"d", "e"}, gottenValues[1])
	assertEquals(t, []any{"f"}, gottenValues[2])
}

func TestBatchCase4(t *testing.T) {
	// both max size and max time are reached and more complex

	in := make(chan any)
	out := make(chan any)

	source := ext.NewChanSource(in)
	batch := flow.NewBatch(3, time.Millisecond*100)

	go ingestSlice([]string{"a", "b"}, in)
	go ingestDeferred("c", in, time.Millisecond*50)

	// batch1: [a b c] (max size)

	// ticker reset at time 50ms

	go ingestDeferred("d", in, time.Millisecond*60)
	go ingestDeferred("e", in, time.Millisecond*140)

	// ticker reached timeout at 50+100=150ms and reset at 150ms
	// so batch2: [d e]

	go ingestDeferred("f", in, time.Millisecond*160)
	go ingestDeferred("g", in, time.Millisecond*175)
	go ingestDeferred("h", in, time.Millisecond*180)

	// ticker reset at 180ms because of the max size reached
	// so batch3: [f g h]

	go ingestDeferred("i", in, time.Millisecond*185)

	// ticker reset at 279ms because of the max size reached
	// so batch4: [i j]

	go ingestDeferred("j", in, time.Millisecond*279)

	// ticker reached timeout at 279+100=379ms and reset at 379ms
	// and this batch is empty, so it will be skipped

	go ingestDeferred("k", in, time.Millisecond*400)
	go ingestDeferred("l", in, time.Millisecond*420)

	// ticker reached timeout at 379+100=479ms and reset at 479ms
	// so batch5: [k l]

	go closeDeferred(in, time.Millisecond*500)

	go func() {
		source.
			Via(batch).
			To(ext.NewChanSink(out))
	}()

	gottenValues := make([][]any, 0)
	for e := range out {
		gottenValues = append(gottenValues, e.([]any))
	}

	assertEquals(t, 5, len(gottenValues))

	assertEquals(t, []any{"a", "b", "c"}, gottenValues[0])
	assertEquals(t, []any{"d", "e"}, gottenValues[1])
	assertEquals(t, []any{"f", "g", "h"}, gottenValues[2])
	assertEquals(t, []any{"i", "j"}, gottenValues[3])
	assertEquals(t, []any{"k", "l"}, gottenValues[4])

}
