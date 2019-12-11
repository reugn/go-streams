package ext

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// PulsarSource connector
type PulsarSource struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	out      chan interface{}
	ctx      context.Context
}

// NewPulsarSource creates a new PulsarSource
func NewPulsarSource(ctx context.Context, clientOptions *pulsar.ClientOptions,
	consumerOptions *pulsar.ConsumerOptions) (*PulsarSource, error) {
	client, err := pulsar.NewClient(*clientOptions)
	if err != nil {
		return nil, err
	}

	consumer, err := client.Subscribe(*consumerOptions)
	if err != nil {
		return nil, err
	}

	source := &PulsarSource{
		client:   client,
		consumer: consumer,
		out:      make(chan interface{}),
		ctx:      ctx,
	}

	go source.init()
	return source, nil
}

// start main loop
func (ps *PulsarSource) init() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
loop:
	for {
		select {
		case <-sigchan:
			break loop
		case <-ps.ctx.Done():
			break loop
		default:
			msg, err := ps.consumer.Receive(ps.ctx)
			if err == nil {
				ps.out <- msg
			} else {
				log.Println(err)
			}
		}
	}
	log.Printf("Closing pulsar consumer")
	close(ps.out)
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
	ctx      context.Context
}

// NewPulsarSink creates a new PulsarSink
func NewPulsarSink(ctx context.Context, clientOptions *pulsar.ClientOptions,
	producerOptions *pulsar.ProducerOptions) (*PulsarSink, error) {
	client, err := pulsar.NewClient(*clientOptions)
	if err != nil {
		return nil, err
	}

	producer, err := client.CreateProducer(*producerOptions)
	if err != nil {
		return nil, err
	}

	sink := &PulsarSink{
		client:   client,
		producer: producer,
		in:       make(chan interface{}),
		ctx:      ctx,
	}

	go sink.init()
	return sink, nil
}

// start main loop
func (ps *PulsarSink) init() {
	for msg := range ps.in {
		switch m := msg.(type) {
		case pulsar.Message:
			ps.producer.Send(ps.ctx, &pulsar.ProducerMessage{
				Payload: m.Payload(),
			})
		case string:
			ps.producer.Send(ps.ctx, &pulsar.ProducerMessage{
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
