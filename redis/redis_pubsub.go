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
	ctx         context.Context
	redisClient *redis.Client
	channel     string
	out         chan interface{}
}

// NewPubSubSource returns a new PubSubSource instance.
//
// The given redisClient is subscribed to the provided channel.
// The replies to subscription and unsubscribing operations are sent in the form of messages
// so that the client reads a coherent stream of messages where the first element
// indicates the type of message.
func NewPubSubSource(ctx context.Context, redisClient *redis.Client, channel string) (*PubSubSource, error) {
	pubsub := redisClient.Subscribe(ctx, channel)

	// Wait for a confirmation that subscription is created before publishing anything
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return nil, err
	}

	source := &PubSubSource{
		ctx:         ctx,
		redisClient: redisClient,
		channel:     channel,
		out:         make(chan interface{}),
	}

	go source.init(pubsub.Channel())
	return source, nil
}

// init starts the main loop
func (ps *PubSubSource) init(ch <-chan *redis.Message) {
loop:
	for {
		select {
		case <-ps.ctx.Done():
			break loop

		case msg := <-ch:
			ps.out <- msg
		}
	}

	log.Printf("Closing Redis Pub/Sub consumer")
	close(ps.out)
	ps.redisClient.Close()
}

// Via streams data through the given flow
func (ps *PubSubSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ps, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (ps *PubSubSource) Out() <-chan interface{} {
	return ps.out
}

// PubSubSink represents a Redis Pub/Sub sink connector.
type PubSubSink struct {
	ctx         context.Context
	redisClient *redis.Client
	channel     string
	in          chan interface{}
}

// NewPubSubSink returns a new PubSubSink instance.
//
// The incoming messages will be published to the given target channel using the
// provided redis.Client.
func NewPubSubSink(ctx context.Context, redisClient *redis.Client, channel string) *PubSubSink {
	sink := &PubSubSink{
		ctx:         ctx,
		redisClient: redisClient,
		channel:     channel,
		in:          make(chan interface{}),
	}

	go sink.init()
	return sink
}

// init starts the main loop
func (ps *PubSubSink) init() {
	for msg := range ps.in {
		switch m := msg.(type) {
		case string:
			err := ps.redisClient.Publish(ps.ctx, ps.channel, m).Err()
			if err != nil {
				log.Printf("Error in redisClient.Publish: %s", err)
			}

		default:
			log.Printf("Unsupported message type %v", m)
		}
	}

	log.Printf("Closing Redis Pub/Sub producer")
	ps.redisClient.Close()
}

// In returns an input channel for receiving data
func (ps *PubSubSink) In() chan<- interface{} {
	return ps.in
}
