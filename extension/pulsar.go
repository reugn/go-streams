package ext

import (
	"context"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// PulsarSource connector
type PulsarSource struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	out      chan interface{}
}

// NewPulsarSource creates a new PulsarSource
func NewPulsarSource(clientOptions *pulsar.ClientOptions, consumerOptions *pulsar.ConsumerOptions) (*PulsarSource, error) {
	client, err := pulsar.NewClient(*clientOptions)
	streams.Check(err)
	consumer, err := client.Subscribe(*consumerOptions)
	streams.Check(err)
	source := &PulsarSource{
		client:   client,
		consumer: consumer,
		out:      make(chan interface{}),
	}
	go source.init()
	return source, nil
}

// start main loop
func (ps *PulsarSource) init() {
	for {
		msg, err := ps.consumer.Receive(context.Background())
		if err == nil {
			ps.out <- msg
		} else {
			log.Fatal(err)
		}
	}
	log.Printf("Closing pulsar consumer")
	ps.consumer.Close()
	ps.client.Close()
}

// Via streams data through given flow
func (ps *PulsarSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ps, _flow)
	return _flow
}

// Out returns channel for sending data
func (ps *PulsarSource) Out() <-chan interface{} {
	return ps.out
}

// PulsarSink connector
type PulsarSink struct {
	client   pulsar.Client
	producer pulsar.Producer
	in       chan interface{}
}

// NewPulsarSink creates a new PulsarSink
func NewPulsarSink(clientOptions *pulsar.ClientOptions, producerOptions *pulsar.ProducerOptions) *PulsarSink {
	client, err := pulsar.NewClient(*clientOptions)
	streams.Check(err)
	producer, err := client.CreateProducer(*producerOptions)
	streams.Check(err)
	sink := &PulsarSink{
		client:   client,
		producer: producer,
		in:       make(chan interface{}),
	}
	go sink.init()
	return sink
}

// start main loop
func (ps *PulsarSink) init() {
	ctx := context.Background()
	for msg := range ps.in {
		switch m := msg.(type) {
		case pulsar.Message:
			ps.producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: m.Payload(),
			})
		case string:
			ps.producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte(m),
			})
		}
	}
	log.Printf("Closing pulsar producer")
	ps.producer.Close()
	ps.client.Close()
}

// In returns channel for receiving data
func (ps *PulsarSink) In() chan<- interface{} {
	return ps.in
}
