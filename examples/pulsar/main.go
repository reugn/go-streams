package main

import (
	"context"
	"log"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/reugn/go-streams/flow"
	ext "github.com/reugn/go-streams/pulsar"
)

func main() {
	clientOptions := pulsar.ClientOptions{URL: "pulsar://localhost:6650"}
	producerOptions := pulsar.ProducerOptions{Topic: "test2"}
	consumerOptions := pulsar.ConsumerOptions{
		Topic:            "test1",
		SubscriptionName: "group1",
		Type:             pulsar.Exclusive,
	}

	ctx := context.Background()
	source, err := ext.NewSource(ctx, &clientOptions, &consumerOptions, nil)
	if err != nil {
		log.Fatal(err)
	}

	toUpperMapFlow := flow.NewMap(toUpper, 1)
	sink, err := ext.NewSink(ctx, &clientOptions, &producerOptions, nil)
	if err != nil {
		log.Fatal(err)
	}

	source.
		Via(toUpperMapFlow).
		To(sink)
}

var toUpper = func(msg pulsar.Message) string {
	return strings.ToUpper(string(msg.Payload()))
}
