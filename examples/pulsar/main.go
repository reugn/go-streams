package main

import (
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/reugn/go-streams"
	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

func main() {
	clientOptions := pulsar.ClientOptions{URL: "pulsar://localhost:6650"}
	producerOptions := pulsar.ProducerOptions{Topic: "test2"}
	consumerOptions := pulsar.ConsumerOptions{
		Topic:            "test1",
		SubscriptionName: "group1",
		Type:             pulsar.Exclusive,
	}

	source, err := ext.NewPulsarSource(&clientOptions, &consumerOptions)
	streams.Check(err)
	flow1 := flow.NewMap(toUpper, 1)
	sink := ext.NewPulsarSink(&clientOptions, &producerOptions)

	source.Via(flow1).To(sink)
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(pulsar.Message)
	return strings.ToUpper(string(msg.Payload()))
}
