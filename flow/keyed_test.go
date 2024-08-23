package flow_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/reugn/go-streams"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

type keyedElement struct {
	key  int
	data string
}

func (e *keyedElement) String() string {
	return e.data
}

func newKeyedElement(key int) *keyedElement {
	return &keyedElement{
		key:  key,
		data: fmt.Sprint(key),
	}
}

func TestKeyed(t *testing.T) {
	in := make(chan any, 30)
	out := make(chan any, 20)

	source := ext.NewChanSource(in)
	sink := ext.NewChanSink(out)
	keyed := flow.NewKeyed(func(e keyedElement) int { return e.key },
		func() streams.Flow {
			return flow.NewBatch[keyedElement](4, 10*time.Millisecond)
		})

	inputValues := values(makeElements(30))
	ingestSlice(inputValues, in)
	close(in)

	go func() {
		source.
			Via(keyed).
			To(sink)
	}()

	outputValues := readSlice[[]keyedElement](sink.Out)

	assert.Equal(t, 20, len(outputValues))

	var sum int
	for _, batch := range outputValues {
		for _, v := range batch {
			sum += v.key
		}
	}
	assert.Equal(t, 292, sum)
}

func TestKeyed_Ptr(t *testing.T) {
	in := make(chan any, 30)
	out := make(chan any, 20)

	source := ext.NewChanSource(in)
	sink := ext.NewChanSink(out)
	keyed := flow.NewKeyed(func(e *keyedElement) int { return e.key },
		func() streams.Flow {
			return flow.NewBatch[*keyedElement](4, 10*time.Millisecond)
		})
	assert.NotEqual(t, keyed.Out(), nil)

	inputValues := makeElements(30)
	ingestSlice(inputValues, in)
	close(in)

	go func() {
		source.
			Via(keyed).
			Via(flow.NewPassThrough()). // Via coverage
			To(sink)
	}()

	outputValues := readSlice[[]*keyedElement](sink.Out)
	fmt.Println(outputValues)

	assert.Equal(t, 20, len(outputValues))

	var sum int
	for _, batch := range outputValues {
		for _, v := range batch {
			sum += v.key
		}
	}
	assert.Equal(t, 292, sum)
}

func TestKeyed_MultipleOperators(t *testing.T) {
	in := make(chan any, 30)
	out := make(chan any, 20)

	source := ext.NewChanSource(in)
	sink := ext.NewChanSink(out)

	keyedFlow := flow.NewKeyed(func(e *keyedElement) int { return e.key },
		func() streams.Flow {
			return flow.NewBatch[*keyedElement](4, 10*time.Millisecond)
		},
		func() streams.Flow {
			return flow.NewMap(func(b []*keyedElement) int {
				var sum int
				for _, v := range b {
					sum += v.key
				}
				return sum
			}, 1)
		})
	collectFlow := flow.NewTumblingWindow[int](25 * time.Millisecond)
	sumFlow := flow.NewMap(sum, 1)

	inputValues := makeElements(30)
	go ingestSlice(inputValues, in)
	go closeDeferred(in, 10*time.Millisecond)

	go func() {
		source.
			Via(keyedFlow).Via(collectFlow).Via(sumFlow).
			To(sink)
	}()

	outputValues := readSlice[int](sink.Out)

	assert.Equal(t, 1, len(outputValues))
	assert.Equal(t, 292, outputValues[0])
}

func TestKeyed_InvalidArguments(t *testing.T) {
	assert.Panics(t, func() {
		flow.NewKeyed(func(e *keyedElement) int { return e.key })
	})
}

func makeElements(n int) []*keyedElement {
	elements := make([]*keyedElement, n)
	denominators := []int{3, 7, 10}
outer:
	for i := range elements {
		for _, d := range denominators {
			if i%d == 0 {
				elements[i] = newKeyedElement(d)
				continue outer
			}
		}
		elements[i] = newKeyedElement(i)
	}
	fmt.Println(elements)
	return elements
}

func values[T any](pointers []*T) []T {
	values := make([]T, len(pointers))
	for i, ptr := range pointers {
		values[i] = *ptr
	}
	return values
}

func sum(values []int) int {
	var result int
	for _, v := range values {
		result += v
	}
	return result
}
