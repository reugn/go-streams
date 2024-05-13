package pulsar

import (
	"context"
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// PulsarSource represents an Apache Pulsar source connector.
type PulsarSource struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	out      chan any
}

var _ streams.Source = (*PulsarSource)(nil)

// NewPulsarSource returns a new PulsarSource connector.
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
		out:      make(chan any),
	}
	go source.init(ctx)

	return source, nil
}

func (ps *PulsarSource) init(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			// this call blocks until a message is available
			msg, err := ps.consumer.Receive(ctx)
			if err != nil {
				log.Printf("Error is Receive: %s", err)
				continue
			}
			ps.out <- msg
		}
	}
	log.Printf("Closing Pulsar source connector")
	close(ps.out)
	ps.consumer.Close()
	ps.client.Close()
}

// Via streams data to a specified operator and returns it.
func (ps *PulsarSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ps, operator)
	return operator
}

// Out returns the output channel of the PulsarSource connector.
func (ps *PulsarSource) Out() <-chan any {
	return ps.out
}

// PulsarSink represents an Apache Pulsar sink connector.
type PulsarSink struct {
	client   pulsar.Client
	producer pulsar.Producer
	in       chan any
}

var _ streams.Sink = (*PulsarSink)(nil)

// NewPulsarSink returns a new PulsarSink connector.
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
		in:       make(chan any),
	}
	go sink.init(ctx)

	return sink, nil
}

func (ps *PulsarSink) init(ctx context.Context) {
	for msg := range ps.in {
		var err error
		switch message := msg.(type) {
		case pulsar.Message:
			_, err = ps.producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: message.Payload(),
			})
		case string:
			_, err = ps.producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte(message),
			})
		default:
			log.Printf("Unsupported message type: %T", message)
		}

		if err != nil {
			log.Printf("Error processing Pulsar message: %s", err)
		}
	}
	log.Printf("Closing Pulsar sink connector")
	ps.producer.Close()
	ps.client.Close()
}

// In returns the input channel of the PulsarSink connector.
func (ps *PulsarSink) In() chan<- any {
	return ps.in
}
