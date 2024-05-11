package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/reugn/go-streams/flow"
	ext "github.com/reugn/go-streams/kafka"
)

func main() {
	hosts := []string{"127.0.0.1:9092"}
	ctx := context.Background()
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Producer.Return.Successes = true
	config.Version, _ = sarama.ParseKafkaVersion("2.8.1")
	groupID := "testConsumer"

	source, err := ext.NewKafkaSource(ctx, hosts, groupID, config, "test")
	if err != nil {
		log.Fatal(err)
	}

	toUpperMapFlow := flow.NewMap(toUpper, 1)
	appendAsteriskFlatMapFlow := flow.NewFlatMap(appendAsterisk, 1)
	sink, err := ext.NewKafkaSink(hosts, config, "test2")
	if err != nil {
		log.Fatal(err)
	}

	throttler := flow.NewThrottler(1, time.Second, 50, flow.Discard)
	tumblingWindow := flow.NewTumblingWindow[*sarama.ConsumerMessage](time.Second * 5)

	source.
		Via(toUpperMapFlow).
		Via(throttler).
		Via(tumblingWindow).
		Via(appendAsteriskFlatMapFlow).
		To(sink)
}

var toUpper = func(msg *sarama.ConsumerMessage) *sarama.ConsumerMessage {
	msg.Value = []byte(strings.ToUpper(string(msg.Value)))
	return msg
}

var appendAsterisk = func(inMessages []*sarama.ConsumerMessage) []*sarama.ConsumerMessage {
	outMessages := make([]*sarama.ConsumerMessage, len(inMessages))
	for i, msg := range inMessages {
		msg.Value = []byte(string(msg.Value) + "*")
		outMessages[i] = msg
	}
	return outMessages
}
