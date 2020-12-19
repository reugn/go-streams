package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
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
	config.Version, _ = sarama.ParseKafkaVersion("2.2.0")
	groupID := "testConsumer"

	source := ext.NewKafkaSource(ctx, hosts, groupID, config, "test")
	flow1 := flow.NewMap(toUpper, 1)
	flow2 := flow.NewFlatMap(appendAsterix, 1)
	sink := ext.NewKafkaSink(hosts, config, "test2")
	throttler := flow.NewThrottler(1, time.Second*1, 50, flow.Discard)
	// slidingWindow := flow.NewSlidingWindow(time.Second*30, time.Second*5)
	tumblingWindow := flow.NewTumblingWindow(time.Second * 5)

	source.Via(flow1).Via(throttler).Via(tumblingWindow).Via(flow2).To(sink)
	wait()
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(*sarama.ConsumerMessage)
	msg.Value = []byte(strings.ToUpper(string(msg.Value)))
	return msg
}

var appendAsterix = func(in interface{}) []interface{} {
	arr := in.([]interface{})
	rt := make([]interface{}, len(arr))
	for i, item := range arr {
		msg := item.(*sarama.ConsumerMessage)
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
