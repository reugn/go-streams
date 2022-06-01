package main

import (
	"context"
	"log"
	"time"

	ext "github.com/reugn/go-streams/aerospike"
	"github.com/reugn/go-streams/util"

	aero "github.com/aerospike/aerospike-client-go/v5"
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
		<-timer.C
		cancelFunc()
	}()

	cnProperties := &ext.ChangeNotificationProperties{
		PollingInterval: time.Second * 3,
	}

	source, err := ext.NewAerospikeSource(ctx, properties, nil, cnProperties)
	if err != nil {
		log.Fatal(err)
	}
	flow1 := flow.NewMap(transform, 1)
	sink, err := ext.NewAerospikeSink(ctx, properties, nil)
	if err != nil {
		log.Fatal(err)
	}

	source.
		Via(flow1).
		To(sink)
}

var transform = func(in interface{}) interface{} {
	msg := in.(*aero.Record)
	log.Println(msg.Bins)
	msg.Bins["ts"] = util.NowNano()
	return ext.AerospikeKeyBins{Key: msg.Key, Bins: msg.Bins}
}
