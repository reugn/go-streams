package pulsar

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// Source represents an Apache Pulsar source connector.
type Source struct {
	client   pulsar.Client
	consumer pulsar.Consumer
	out      chan any

	logger *slog.Logger
}

var _ streams.Source = (*Source)(nil)

// NewSource returns a new [Source] connector.
func NewSource(ctx context.Context, clientOptions *pulsar.ClientOptions,
	consumerOptions *pulsar.ConsumerOptions, logger *slog.Logger) (*Source, error) {
	client, err := pulsar.NewClient(*clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	consumer, err := client.Subscribe(*consumerOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "pulsar"),
		slog.String("type", "source")))

	source := &Source{
		client:   client,
		consumer: consumer,
		out:      make(chan any),
		logger:   logger,
	}

	// asynchronously consume data and send it downstream
	go source.process(ctx)

	return source, nil
}

func (ps *Source) process(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			// this call blocks until a message is available
			msg, err := ps.consumer.Receive(ctx)
			if err != nil {
				ps.logger.Error("Error in consumer.Receive",
					slog.Any("error", err))
				continue
			}
			ps.out <- msg
		}
	}

	ps.logger.Info("Closing connector")
	close(ps.out)
	ps.consumer.Close()
	ps.client.Close()
}

// Via asynchronously streams data to the given Flow and returns it.
func (ps *Source) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ps, operator)
	return operator
}

// Out returns the output channel of the Source connector.
func (ps *Source) Out() <-chan any {
	return ps.out
}

// Sink represents an Apache Pulsar sink connector.
type Sink struct {
	client   pulsar.Client
	producer pulsar.Producer
	in       chan any

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*Sink)(nil)

// NewSink returns a new [Sink] connector.
func NewSink(ctx context.Context, clientOptions *pulsar.ClientOptions,
	producerOptions *pulsar.ProducerOptions, logger *slog.Logger) (*Sink, error) {
	client, err := pulsar.NewClient(*clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	producer, err := client.CreateProducer(*producerOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "pulsar"),
		slog.String("type", "sink")))

	sink := &Sink{
		client:   client,
		producer: producer,
		in:       make(chan any),
		done:     make(chan struct{}),
		logger:   logger,
	}

	// begin processing upstream data
	go sink.process(ctx)

	return sink, nil
}

func (ps *Sink) process(ctx context.Context) {
	defer close(ps.done) // signal data processing completion

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
			ps.logger.Error("Unsupported message type",
				slog.String("type", fmt.Sprintf("%T", message)))
		}

		if err != nil {
			ps.logger.Error("Error processing message", slog.Any("error", err))
		}
	}

	ps.logger.Info("Closing connector")
	ps.producer.Close()
	ps.client.Close()
}

// In returns the input channel of the Sink connector.
func (ps *Sink) In() chan<- any {
	return ps.in
}

// AwaitCompletion blocks until the Sink connector has completed
// processing all the received data.
func (ps *Sink) AwaitCompletion() {
	<-ps.done
}
