package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/internal/util"
)

// Test producer: nc -u 127.0.0.1 3434
// Test consumer: nc -u -l 3535
func main() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	timer := time.NewTimer(time.Minute)
	go func() {
		select {
		case <-timer.C:
			cancelFunc()
		}
	}()

	source, err := ext.NewNetSource(ctx, ext.UDP, "127.0.0.1:3434")
	util.Check(err)
	flow1 := flow.NewMap(toUpper, 1)
	sink, err := ext.NewNetSink(ext.UDP, "127.0.0.1:3535")
	util.Check(err)

	source.Via(flow1).To(sink)
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(string)
	fmt.Printf("Got: %s", msg)
	return strings.ToUpper(msg)
}
