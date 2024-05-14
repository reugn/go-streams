package redis

import (
	"context"
	"log"

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
}

var _ streams.Source = (*PubSubSource)(nil)

// NewPubSubSource returns a new PubSubSource instance.
//
// The given redisClient is subscribed to the provided channel.
// The replies to subscription and unsubscribing operations are sent in the form
// of messages so that the client reads a coherent stream of messages where the
// first element indicates the type of message.
func NewPubSubSource(ctx context.Context, redisClient *redis.Client,
	channel string) (*PubSubSource, error) {
	pubsub := redisClient.Subscribe(ctx, channel)

	// wait for a confirmation that subscription is created before
	// publishing anything
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return nil, err
	}

	source := &PubSubSource{
		redisClient: redisClient,
		channel:     channel,
		out:         make(chan any),
	}
	go source.init(ctx, pubsub.Channel())

	return source, nil
}

func (ps *PubSubSource) init(ctx context.Context, ch <-chan *redis.Message) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		// route incoming messages downstream
		case msg := <-ch:
			ps.out <- msg
		}
	}
	log.Printf("Closing Redis PubSubSource connector")
	close(ps.out)
	if err := ps.redisClient.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// Via streams data to a specified operator and returns it.
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
}

var _ streams.Sink = (*PubSubSink)(nil)

// NewPubSubSink returns a new PubSubSink instance.
//
// The incoming messages will be published to the given target channel using
// the provided redis.Client.
func NewPubSubSink(ctx context.Context, redisClient *redis.Client,
	channel string) *PubSubSink {
	sink := &PubSubSink{
		redisClient: redisClient,
		channel:     channel,
		in:          make(chan any),
	}
	go sink.init(ctx)

	return sink
}

func (ps *PubSubSink) init(ctx context.Context) {
	for msg := range ps.in {
		switch message := msg.(type) {
		case string:
			if err := ps.redisClient.Publish(ctx, ps.channel, message).Err(); err != nil {
				log.Printf("Error in Publish: %s", err)
			}
		default:
			log.Printf("Unsupported message type: %T", message)
		}
	}
	log.Printf("Closing Redis PubSubSink connector")
	if err := ps.redisClient.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// In returns the input channel of the PubSubSink connector.
func (ps *PubSubSink) In() chan<- any {
	return ps.in
}
