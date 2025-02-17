package main

import (
	"fmt"
	"strconv"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

type message struct {
	msg string
}

func (msg *message) String() string {
	return msg.msg
}

func main() {
	source := ext.NewChanSource(tickerChan(time.Second))
	mapFlow := flow.NewMap(quote, 1)
	sink := ext.NewStdoutSink()

	source.
		Via(mapFlow).
		To(sink)
}

func quote(msg *message) *message {
	msg.msg = fmt.Sprintf("%q", msg.msg)
	return msg
}

func tickerChan(interval time.Duration) chan any {
	outChan := make(chan any)
	go func() {
		ticker := time.NewTicker(interval)
		for t := range ticker.C {
			outChan <- &message{msg: strconv.FormatInt(t.UnixMilli(), 10)}
		}
	}()
	return outChan
}
