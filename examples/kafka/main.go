package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func main() {
	host := "127.0.0.1:9092"
	config := kafka.ConfigMap{
		"bootstrap.servers": host,
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	}

	source := ext.NewKafkaSource(&config, "test")
	flow := flow.NewMap(toUpper, 1)
	sink := ext.NewKafkaSink(&config, "test2")

	source.Via(flow).To(sink)
	wait()
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(*kafka.Message)
	msg.Value = []byte(strings.ToUpper(string(msg.Value)))
	return msg
}

func wait() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		return
	}
}
