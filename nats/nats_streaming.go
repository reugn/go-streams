package nats

import (
	"context"
	"log"

	stan "github.com/nats-io/stan.go"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// StreamingSource represents a NATS Streaming source connector.
// Deprecated: Use [JetStreamSource] instead.
type StreamingSource struct {
	conn             stan.Conn
	subscriptions    []stan.Subscription
	subscriptionType stan.SubscriptionOption
	topics           []string
	out              chan any
}

var _ streams.Source = (*StreamingSource)(nil)

// NewStreamingSource returns a new StreamingSource connector.
func NewStreamingSource(ctx context.Context, conn stan.Conn,
	subscriptionType stan.SubscriptionOption,
	topics ...string) *StreamingSource {
	streamingSource := &StreamingSource{
		conn:             conn,
		subscriptions:    []stan.Subscription{},
		subscriptionType: subscriptionType,
		topics:           topics,
		out:              make(chan any),
	}
	go streamingSource.init(ctx)

	return streamingSource
}

func (ns *StreamingSource) init(ctx context.Context) {
	// bind all topic subscribers
	for _, topic := range ns.topics {
		sub, err := ns.conn.Subscribe(topic, func(msg *stan.Msg) {
			ns.out <- msg
		}, ns.subscriptionType)
		if err != nil {
			log.Printf("Failed to subscribe to topic %s: %s", topic, err)
			continue
		}
		log.Printf("Subscribed to topic %s", topic)
		ns.subscriptions = append(ns.subscriptions, sub)
	}

	<-ctx.Done()

	log.Printf("Closing NATS Streaming source connector")
	close(ns.out)
	ns.unsubscribe() // unbind all topic subscriptions
	if err := ns.conn.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

func (ns *StreamingSource) unsubscribe() {
	for _, subscription := range ns.subscriptions {
		if err := subscription.Unsubscribe(); err != nil {
			log.Printf("Failed to remove NATS subscription: %s", err)
		}
	}
}

// Via streams data to a specified operator and returns it.
func (ns *StreamingSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ns, operator)
	return operator
}

// Out returns the output channel of the StreamingSource connector.
func (ns *StreamingSource) Out() <-chan any {
	return ns.out
}

// StreamingSink represents a NATS Streaming sink connector.
// Deprecated: Use [JetStreamSink] instead.
type StreamingSink struct {
	conn  stan.Conn
	topic string
	in    chan any
}

var _ streams.Sink = (*StreamingSink)(nil)

// NewStreamingSink returns a new StreamingSink connector.
func NewStreamingSink(conn stan.Conn, topic string) *StreamingSink {
	streamingSink := &StreamingSink{
		conn:  conn,
		topic: topic,
		in:    make(chan any),
	}
	go streamingSink.init()

	return streamingSink
}

func (ns *StreamingSink) init() {
	for msg := range ns.in {
		var err error
		switch message := msg.(type) {
		case *stan.Msg:
			err = ns.conn.Publish(ns.topic, message.Data)
		case []byte:
			err = ns.conn.Publish(ns.topic, message)
		default:
			log.Printf("Unsupported message type: %T", message)
		}

		if err != nil {
			log.Printf("Error processing NATS Streaming message: %s", err)
		}
	}
	log.Printf("Closing NATS Streaming sink connector")
	if err := ns.conn.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// In returns the input channel of the StreamingSink connector.
func (ns *StreamingSink) In() chan<- any {
	return ns.in
}
