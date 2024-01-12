package nats

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

var (
	PullMaxWaiting = 128
	FetchBatchSize = 16
)

// JetStreamSource represents a NATS JetStream source connector.
// Uses a pull-based consumer.
type JetStreamSource struct {
	conn             *nats.Conn
	jetStreamContext nats.JetStreamContext
	subscription     *nats.Subscription
	out              chan any
	ctx              context.Context
}

// NewNatsSink returns a new JetStreamSource instance.
func NewJetStreamSource(ctx context.Context, subjectName, url string) (*JetStreamSource, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	// create JetStreamContext
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	// create pull based consumer
	sub, err := js.PullSubscribe(subjectName, "JetStreamSource", nats.PullMaxWaiting(PullMaxWaiting))
	if err != nil {
		return nil, err
	}

	jetStreamSource := &JetStreamSource{
		conn:             nc,
		jetStreamContext: js,
		subscription:     sub,
		out:              make(chan any),
		ctx:              ctx,
	}

	go jetStreamSource.init()
	return jetStreamSource, nil
}

func (js *JetStreamSource) init() {
loop:
	for {
		select {
		case <-js.ctx.Done():
			break loop
		default:
		}

		messages, err := js.subscription.Fetch(FetchBatchSize, nats.Context(js.ctx))
		if err != nil {
			log.Printf("JetStreamSource fetch error: %s", err)
			break loop
		}
		for _, msg := range messages {
			if err := msg.Ack(); err != nil {
				log.Printf("Failed to Ack JetStream message: %s", err)
			}
			js.out <- msg
		}
	}

	log.Printf("Closing JetStream consumer")
	if err := js.subscription.Drain(); err != nil {
		log.Printf("Failed to Drain JetStream subscription: %s", err)
	}
	close(js.out)
}

// Via streams data through the given flow
func (js *JetStreamSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(js, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (js *JetStreamSource) Out() <-chan any {
	return js.out
}

// JetStreamSink represents a NATS JetStream sink connector.
type JetStreamSink struct {
	conn             *nats.Conn
	jetStreamContext nats.JetStreamContext
	subjectName      string
	in               chan any
}

// NewNatsSink returns a new JetStreamSource instance.
func NewJetStreamSink(streamName, subjectName, url string) (*JetStreamSink, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}

	// create JetStreamContext
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	// check if the given stream already exists; if not, create it.
	stream, _ := js.StreamInfo(streamName)
	if stream == nil {
		log.Printf("Creating JetStream %s with subject %s", streamName, subjectName)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{subjectName},
		})
		if err != nil {
			return nil, err
		}
	} else {
		log.Printf("JetStream %s exists", streamName)
	}

	jetStreamSink := &JetStreamSink{
		conn:             nc,
		jetStreamContext: js,
		subjectName:      subjectName,
		in:               make(chan any),
	}

	go jetStreamSink.init()
	return jetStreamSink, nil
}

func (js *JetStreamSink) init() {
	for msg := range js.in {
		var err error
		switch m := msg.(type) {
		case *nats.Msg:
			_, err = js.jetStreamContext.Publish(js.subjectName, m.Data)

		case []byte:
			_, err = js.jetStreamContext.Publish(js.subjectName, m)

		default:
			log.Printf("Unsupported message type %v", m)
		}

		if err != nil {
			log.Printf("Error processing JetStream message: %s", err)
		}
	}

	log.Printf("Closing JetStream producer")
	if err := js.conn.Drain(); err != nil {
		log.Printf("Failed to Drain JetStream connection: %s", err)
	}
}

// In returns an input channel for receiving data
func (js *JetStreamSink) In() chan<- any {
	return js.in
}
