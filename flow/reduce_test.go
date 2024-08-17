package flow_test

import (
	"testing"

	"github.com/reugn/go-streams"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

func TestReduce(t *testing.T) {
	tests := []struct {
		name       string
		reduceFlow streams.Flow
		ptr        bool
	}{
		{
			name: "values",
			reduceFlow: flow.NewReduce(func(a int, b int) int {
				return a + b
			}),
			ptr: false,
		},
		{
			name: "pointers",
			reduceFlow: flow.NewReduce(func(a *int, b *int) *int {
				result := *a + *b
				return &result
			}),
			ptr: true,
		},
	}
	input := []int{1, 2, 3, 4, 5}
	expected := []int{1, 3, 6, 10, 15}
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
				Via(tt.reduceFlow).
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
