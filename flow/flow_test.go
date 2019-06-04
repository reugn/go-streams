package flow_test

import (
	"reflect"
	"strings"
	"testing"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

var toUpper = func(in interface{}) interface{} {
	msg := in.(string)
	return strings.ToUpper(msg)
}

func ingest(source []string, in chan interface{}) {
	for _, e := range source {
		in <- e
	}
	close(in)
}

func TestMapFlow(t *testing.T) {
	in := make(chan interface{})
	out := make(chan interface{})

	source := ext.NewChanSource(in)
	flow1 := flow.NewMap(toUpper, 1)
	sink := ext.NewChanSink(out)

	var _input = []string{"a", "b", "c"}
	var _expectedOutput = []string{"A", "B", "C"}

	go ingest(_input, in)
	go func() {
		source.Via(flow1).To(sink)
	}()
	var _output []string
	for e := range sink.Out {
		_output = append(_output, e.(string))
	}
	assertEqual(t, _expectedOutput, _output)
}

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("%s != %s", a, b)
	}
}
