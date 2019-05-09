package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func main() {
	host := "127.0.0.1:9092"
	config := kafka.ConfigMap{
		"bootstrap.servers":  host,
		"group.id":           "myGroup",
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	}

	source := ext.NewKafkaSource(&config, "test")
	flow1 := flow.NewMap(toUpper, 1)
	flow2 := flow.NewFlatMap(appendAsterix, 1)
	sink := ext.NewKafkaSink(&config, "test2")
	throttler := flow.NewThrottler(1, time.Second*1, 50, flow.Discard)
	// slidingWindow := flow.NewSlidingWindow(time.Second*30, time.Second*5)
	tumblingWindow := flow.NewTumblingWindow(time.Second * 5)
	//manual offset commit flow
	commit := source.Commit()

	source.Via(flow1).Via(commit).Via(throttler).Via(tumblingWindow).Via(flow2).To(sink)
	wait()
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(*kafka.Message)
	msg.Value = []byte(strings.ToUpper(string(msg.Value)))
	return msg
}

var appendAsterix = func(in interface{}) []interface{} {
	arr := in.([]interface{})
	rt := make([]interface{}, len(arr))
	for i, item := range arr {
		msg := item.(*kafka.Message)
		msg.Value = []byte(string(msg.Value) + "*")
		rt[i] = msg
	}
	return rt
}

func wait() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		return
	}
}
