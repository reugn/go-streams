package main

import (
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func main() {
	source := ext.NewFileSource("in.txt")
	reverseMapFlow := flow.NewMap(reverse, 1)
	sink := ext.NewFileSink("out.txt")

	source.
		Via(reverseMapFlow).
		To(sink)

	time.Sleep(time.Second)
}

var reverse = func(str string) string {
	var reverse string
	for i := len(str) - 1; i >= 0; i-- {
		reverse += string(str[i])
	}
	return reverse
}
