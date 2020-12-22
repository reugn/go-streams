package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/stan.go"
	ext "github.com/reugn/go-streams/nats"

	"github.com/reugn/go-streams/flow"
)

func main() {
	subConn, err := stan.Connect("test-cluster", "test-subscriber", stan.NatsURL("nats://localhost:4222"))

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	log.Println("Connected to nats streaming server")

	ctx := context.Background()
	source := ext.NewNatsSource(ctx, subConn, "input-topic")
	flow1 := flow.NewMap(toUpper, 1)
	flow2 := flow.NewFlatMap(appendAsterix, 1)

	prodConn, err := stan.Connect("test-cluster", "test-producer", stan.NatsURL("nats://localhost:4222"))

	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	sink := ext.NewNatsSink(prodConn, "output-topic")
	throttler := flow.NewThrottler(1, time.Second*1, 50, flow.Discard)
	tumblingWindow := flow.NewTumblingWindow(time.Second * 5)

	source.Via(flow1).Via(throttler).Via(tumblingWindow).Via(flow2).To(sink)
	wait()
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(*stan.Msg)
	msg.Data = []byte(strings.ToUpper(string(msg.Data)))
	return msg
}

var appendAsterix = func(in interface{}) []interface{} {
	arr := in.([]interface{})
	rt := make([]interface{}, len(arr))
	for i, item := range arr {
		msg := item.(*stan.Msg)
		msg.Data = []byte(string(msg.Data) + "*")
		rt[i] = msg
	}
	return rt
}

func wait() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		log.Println("shutting down")
		return
	}
}
