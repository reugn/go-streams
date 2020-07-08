package main

import (
	"context"
	"fmt"
	"time"

	aero "github.com/aerospike/aerospike-client-go"
	"github.com/reugn/go-streams"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func main() {
	properties := &ext.AerospikeProperties{
		Policy:    nil,
		Hostname:  "localhost",
		Port:      3000,
		Namespase: "test",
		SetName:   "streams",
	}
	ctx, cancelFunc := context.WithCancel(context.Background())

	timer := time.NewTimer(time.Minute)
	go func() {
		select {
		case <-timer.C:
			cancelFunc()
		}
	}()

	cnProperties := &ext.ChangeNotificationProperties{PollingInterval: time.Second * 3}
	source, err := ext.NewAerospikeSource(ctx, properties, nil, cnProperties)
	streams.Check(err)
	flow1 := flow.NewMap(transform, 1)
	sink, err := ext.NewAerospikeSink(ctx, properties, nil)
	streams.Check(err)

	source.Via(flow1).To(sink)
}

var transform = func(in interface{}) interface{} {
	msg := in.(*aero.Record)
	fmt.Println(msg.Bins)
	msg.Bins["ts"] = streams.NowNano()
	return ext.AerospikeKeyBins{Key: msg.Key, Bins: msg.Bins}
}
