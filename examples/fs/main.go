package main

import (
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func main() {
	source := ext.NewFileSource("in.txt")
	flow := flow.NewMap(reverse, 1)
	sink := ext.NewFileSink("out.txt")

	source.Via(flow).To(sink)

	time.Sleep(time.Second)
}

var reverse = func(in interface{}) interface{} {
	s := in.(string)
	var reverse string
	for i := len(s) - 1; i >= 0; i-- {
		reverse += string(s[i])
	}
	return reverse
}
