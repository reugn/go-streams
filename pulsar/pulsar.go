package pulsar

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

// PulsarSource represents an Apache Pulsar source connector.
type PulsarSource struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	out      chan interface{}
	ctx      context.Context
}

// NewPulsarSource returns a new PulsarSource instance.
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

// init starts the main loop
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

	log.Printf("Closing Pulsar consumer")
	close(ps.out)
	ps.consumer.Close()
	ps.client.Close()
}

// Via streams data through the given flow
func (ps *PulsarSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ps, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (ps *PulsarSource) Out() <-chan interface{} {
	return ps.out
}

// PulsarSink represents an Apache Pulsar sink connector.
type PulsarSink struct {
	client   pulsar.Client
	producer pulsar.Producer
	in       chan interface{}
	ctx      context.Context
}

// NewPulsarSink returns a new PulsarSink instance.
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

// init starts the main loop
func (ps *PulsarSink) init() {
	for msg := range ps.in {
		var err error
		switch m := msg.(type) {
		case pulsar.Message:
			_, err = ps.producer.Send(ps.ctx, &pulsar.ProducerMessage{
				Payload: m.Payload(),
			})

		case string:
			_, err = ps.producer.Send(ps.ctx, &pulsar.ProducerMessage{
				Payload: []byte(m),
			})

		default:
			log.Printf("Unsupported message type %v", m)
		}

		if err != nil {
			log.Printf("Error processing Pulsar message: %s", err)
		}
	}

	log.Printf("Closing Pulsar producer")
	ps.producer.Close()
	ps.client.Close()
}

// In returns an input channel for receiving data
func (ps *PulsarSink) In() chan<- interface{} {
	return ps.in
}
