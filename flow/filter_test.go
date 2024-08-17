package flow_test

import (
	"testing"

	"github.com/reugn/go-streams"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		name       string
		filterFlow streams.Flow
		ptr        bool
	}{
		{
			name: "values",
			filterFlow: flow.NewFilter(func(e int) bool {
				return e%2 != 0
			}, 1),
			ptr: false,
		},
		{
			name: "pointers",
			filterFlow: flow.NewFilter(func(e *int) bool {
				return *e%2 != 0
			}, 1),
			ptr: true,
		},
	}
	input := []int{1, 2, 3, 4, 5}
	expected := []int{1, 3, 5}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make(chan any, 5)
			out := make(chan any, 5)

			source := ext.NewChanSource(in)
			sink := ext.NewChanSink(out)

			if tt.ptr {
				ingestSlice(ptrSlice(input), in)
			} else {
				ingestSlice(input, in)
			}
			close(in)

			source.
				Via(tt.filterFlow).
				To(sink)

			if tt.ptr {
				output := readSlicePtr[int](out)
				assert.Equal(t, ptrSlice(expected), output)
			} else {
				output := readSlice[int](out)
				assert.Equal(t, expected, output)
			}
		})
	}
}

func TestFilter_NonPositiveParallelism(t *testing.T) {
	assert.Panics(t, func() {
		flow.NewFilter(filterNotContainsA, 0)
	})
	assert.Panics(t, func() {
		flow.NewFilter(filterNotContainsA, -1)
	})
}
