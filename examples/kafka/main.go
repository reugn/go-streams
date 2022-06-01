package main

import (
	"context"
	"log"
	"strings"
	"time"

	ext "github.com/reugn/go-streams/kafka"

	"github.com/Shopify/sarama"
	"github.com/reugn/go-streams/flow"
)

func main() {
	hosts := []string{"127.0.0.1:9092"}
	ctx := context.Background()
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Producer.Return.Successes = true
	config.Version, _ = sarama.ParseKafkaVersion("2.8.1")
	groupID := "testConsumer"

	source, err := ext.NewKafkaSource(ctx, hosts, groupID, config, "test")
	if err != nil {
		log.Fatal(err)
	}
	flow1 := flow.NewMap(toUpper, 1)
	flow2 := flow.NewFlatMap(appendAsterisk, 1)
	sink, err := ext.NewKafkaSink(hosts, config, "test2")
	if err != nil {
		log.Fatal(err)
	}
	throttler := flow.NewThrottler(1, time.Second, 50, flow.Discard)
	// slidingWindow := flow.NewSlidingWindow(time.Second*30, time.Second*5)
	tumblingWindow := flow.NewTumblingWindow(time.Second * 5)

	source.
		Via(flow1).
		Via(throttler).
		Via(tumblingWindow).
		Via(flow2).
		To(sink)
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(*sarama.ConsumerMessage)
	msg.Value = []byte(strings.ToUpper(string(msg.Value)))
	return msg
}

var appendAsterisk = func(in interface{}) []interface{} {
	arr := in.([]interface{})
	outArr := make([]interface{}, len(arr))
	for i, item := range arr {
		msg := item.(*sarama.ConsumerMessage)
		msg.Value = []byte(string(msg.Value) + "*")
		outArr[i] = msg
	}
	return outArr
}
