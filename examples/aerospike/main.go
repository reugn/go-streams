package main

import (
	"context"
	"log"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/reugn/go-streams/aerospike"
	"github.com/reugn/go-streams/flow"
)

func main() {
	client, err := aero.NewClient("localhost", 3000)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	timer := time.NewTimer(time.Minute)
	go func() {
		<-timer.C
		cancelFunc()
	}()

	queryPolicy := aero.NewQueryPolicy()
	queryPolicy.SendKey = true // send user defined key
	source := aerospike.NewPollingSource(ctx, client, aerospike.PollingConfig{
		PollingInterval: 5 * time.Second,
		QueryPolicy:     queryPolicy,
		Namespace:       "test",
		SetName:         "source",
	}, nil)

	mapFlow := flow.NewMap(transform, 1)

	batchWritePolicy := aero.NewBatchWritePolicy()
	batchWritePolicy.SendKey = true // send user defined key
	sink := aerospike.NewSink(client, aerospike.SinkConfig{
		BatchSize:           3,
		BufferFlushInterval: 10 * time.Second,
		BatchWritePolicy:    batchWritePolicy,
		Namespace:           "test",
		SetName:             "sink",
	}, nil)

	source.
		Via(mapFlow).
		To(sink)

	client.Close() // close the Aerospike client
}

var transform = func(rec *aero.Record) aerospike.Record {
	rec.Bins["ts"] = time.Now().UnixNano()
	key, err := aero.NewKey(rec.Key.Namespace(), "sink", rec.Key.Value())
	if err != nil {
		log.Fatal(err)
	}
	return aerospike.Record{
		Key:  key,
		Bins: rec.Bins,
	}
}
