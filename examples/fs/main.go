package main

import (
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func main() {
	source := ext.NewFileSource("in.txt")
	reverseMapFlow := flow.NewMap(reverseString, 1)
	newLineMapFlow := flow.NewMap(addNewLine, 1)
	sink := ext.NewFileSink("out.txt")

	source.
		Via(reverseMapFlow).
		Via(newLineMapFlow).
		To(sink)
}

func reverseString(s string) string {
	runes := []rune(s)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes)
}

func addNewLine(s string) string {
	return s + "\n"
}
