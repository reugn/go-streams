package main

import (
	"strconv"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

type message struct {
	Msg string
}

func (msg *message) String() string {
	return msg.Msg
}

func main() {

	source := ext.NewChanSource(tickerChan(time.Second * 1))
	mapFlow := flow.NewMap(addUTC, 1)
	sink := ext.NewStdoutSink()

	source.
		Via(mapFlow).
		To(sink)
}

var addUTC = func(msg *message) *message {
	msg.Msg += "-UTC"
	return msg
}

func tickerChan(repeat time.Duration) chan interface{} {
	ticker := time.NewTicker(repeat)
	oc := ticker.C
	nc := make(chan interface{})
	go func() {
		for range oc {
			nc <- &message{strconv.FormatInt(time.Now().UnixNano(), 10)}
		}
	}()
	return nc
}
