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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	hosts := []string{"127.0.0.1:9092"}
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Producer.Return.Successes = true
	config.Version, _ = sarama.ParseKafkaVersion("3.6.2")
	groupID := "testConsumer"

	consumerGroup, err := sarama.NewConsumerGroup(hosts, groupID, config)
	if err != nil {
		log.Fatal(err)
	}

	source := ext.NewSaramaSource(ctx, consumerGroup, []string{"test"}, nil)

	syncProducer, err := sarama.NewSyncProducer(hosts, config)
	if err != nil {
		log.Fatal(err)
	}

	sink := ext.NewSaramaSink(syncProducer, "test2", nil)

	toUpperMapFlow := flow.NewMap(toUpper, 1)
	throttler := flow.NewThrottler(1, time.Second, 50, flow.Discard)
	tumblingWindow := flow.NewTumblingWindow[*sarama.ConsumerMessage](time.Second * 5)
	appendAsteriskFlatMapFlow := flow.NewFlatMap(appendAsterisk, 1)

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
