package redis

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/redis/go-redis/v9"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// PubSubSource represents a Redis Pub/Sub source connector.
//
// In the Publish/Subscribe messaging paradigm senders (publishers)
// are not programmed to send their messages to specific receivers (subscribers).
// Rather, published messages are characterized into channels, without knowledge
// of what (if any) subscribers there may be.
type PubSubSource struct {
	redisClient *redis.Client
	channel     string
	out         chan any

	logger *slog.Logger
}

var _ streams.Source = (*PubSubSource)(nil)

// NewPubSubSource returns a new [PubSubSource] connector.
//
// The given redisClient is subscribed to the provided channel.
// The replies to subscription and unsubscribing operations are sent in the form
// of messages so that the client reads a coherent stream of messages where the
// first element indicates the type of message.
func NewPubSubSource(ctx context.Context, redisClient *redis.Client,
	channel string, logger *slog.Logger) (*PubSubSource, error) {
	pubSub := redisClient.Subscribe(ctx, channel)

	// wait for a confirmation that subscription is created before
	// publishing anything
	if _, err := pubSub.Receive(ctx); err != nil {
		return nil, fmt.Errorf("failed to receive: %w", err)
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "redis.pubsub"),
		slog.String("type", "source")))

	source := &PubSubSource{
		redisClient: redisClient,
		channel:     channel,
		out:         make(chan any),
		logger:      logger,
	}

	// asynchronously consume data and send it downstream
	go source.process(ctx, pubSub.Channel())

	return source, nil
}

func (ps *PubSubSource) process(ctx context.Context, ch <-chan *redis.Message) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		// route incoming messages downstream
		case message := <-ch:
			ps.out <- message
		}
	}

	ps.logger.Info("Closing connector")
	close(ps.out)

	if err := ps.redisClient.Close(); err != nil {
		ps.logger.Warn("Error in client.Close", slog.Any("error", err))
	}
}

// Via asynchronously streams data to the given Flow and returns it.
func (ps *PubSubSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ps, operator)
	return operator
}

// Out returns the output channel of the PubSubSource connector.
func (ps *PubSubSource) Out() <-chan any {
	return ps.out
}

// PubSubSink represents a Redis Pub/Sub sink connector.
type PubSubSink struct {
	redisClient *redis.Client
	channel     string
	in          chan any

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*PubSubSink)(nil)

// NewPubSubSink returns a new [PubSubSink] connector.
//
// The incoming messages will be published to the given target channel using
// the provided redis.Client.
func NewPubSubSink(ctx context.Context, redisClient *redis.Client,
	channel string, logger *slog.Logger) *PubSubSink {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "redis.pubsub"),
		slog.String("type", "sink")))

	sink := &PubSubSink{
		redisClient: redisClient,
		channel:     channel,
		in:          make(chan any),
		done:        make(chan struct{}),
		logger:      logger,
	}

	// begin processing upstream data
	go sink.process(ctx)

	return sink
}

func (ps *PubSubSink) process(ctx context.Context) {
	defer close(ps.done) // signal data processing completion

	for msg := range ps.in {
		switch message := msg.(type) {
		case string:
			if err := ps.redisClient.Publish(ctx, ps.channel, message).Err(); err != nil {
				ps.logger.Error("Error in client.Publish", slog.Any("error", err))
			}
		default:
			ps.logger.Error("Unsupported message type",
				slog.String("type", fmt.Sprintf("%T", message)))
		}
	}

	ps.logger.Info("Closing connector")
	if err := ps.redisClient.Close(); err != nil {
		ps.logger.Warn("Error in client.Close", slog.Any("error", err))
	}
}

// In returns the input channel of the PubSubSink connector.
func (ps *PubSubSink) In() chan<- any {
	return ps.in
}

// AwaitCompletion blocks until the PubSubSink connector has completed
// processing all the received data.
func (ps *PubSubSink) AwaitCompletion() {
	<-ps.done
}
