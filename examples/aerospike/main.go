package main

import (
	"context"
	"log"
	"time"

	ext "github.com/reugn/go-streams/aerospike"

	aero "github.com/aerospike/aerospike-client-go/v6"
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

	mapFlow := flow.NewMap(transform, 1)
	sink, err := ext.NewAerospikeSink(ctx, properties, nil)
	if err != nil {
		log.Fatal(err)
	}

	source.
		Via(mapFlow).
		To(sink)
}

var transform = func(msg *aero.Record) ext.AerospikeKeyBins {
	log.Println(msg.Bins)
	msg.Bins["ts"] = time.Now().UnixNano()
	return ext.AerospikeKeyBins{
		Key:  msg.Key,
		Bins: msg.Bins,
	}
}
