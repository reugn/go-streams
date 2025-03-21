package flow_test

import (
	"strconv"
	"testing"

	"github.com/reugn/go-streams"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/assert"
)

func TestFold(t *testing.T) {
	tests := []struct {
		name     string
		foldFlow streams.Flow
	}{
		{
			name: "strings",
			foldFlow: flow.NewFold(
				"",
				func(a int, b string) string {
					return b + strconv.Itoa(a)
				}),
		},
	}
	input := []int{1, 2, 3, 4, 5}
	expected := []string{"1", "12", "123", "1234", "12345"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make(chan any, 5)
			out := make(chan any, 5)

			source := ext.NewChanSource(in)
			sink := ext.NewChanSink(out)

			ingestSlice(input, in)
			close(in)

			source.
				Via(tt.foldFlow).
				Via(flow.NewPassThrough()). // Via coverage
				To(sink)

			output := readSlice[string](out)
			assert.Equal(t, expected, output)
		})
	}
}
