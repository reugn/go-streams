package nats

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	stan "github.com/nats-io/stan.go"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// StreamingSource represents a NATS Streaming source connector.
// Deprecated: Use JetStreamSource instead.
type StreamingSource struct {
	conn             stan.Conn
	subscriptions    []stan.Subscription
	subscriptionType stan.SubscriptionOption

	topics    []string
	out       chan any
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        *sync.WaitGroup
}

// NewStreamingSource returns a new StreamingSource connector.
func NewStreamingSource(ctx context.Context, conn stan.Conn, subscriptionType stan.SubscriptionOption,
	topics ...string) *StreamingSource {
	cctx, cancel := context.WithCancel(ctx)

	streamingSource := &StreamingSource{
		conn:             conn,
		subscriptions:    []stan.Subscription{},
		subscriptionType: subscriptionType,
		topics:           topics,
		out:              make(chan any),
		ctx:              cctx,
		cancelCtx:        cancel,
		wg:               &sync.WaitGroup{},
	}

	go streamingSource.init()
	return streamingSource
}

// init starts the stream processing loop.
func (ns *StreamingSource) init() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// bind all topic subscribers
	for _, topic := range ns.topics {
		ns.wg.Add(1)
		go func(t string) {
			defer ns.wg.Done()
			sub, err := ns.conn.Subscribe(t, func(msg *stan.Msg) {
				ns.out <- msg
			}, ns.subscriptionType)
			if err != nil {
				log.Fatal("Failed to subscribe to NATS cluster")
			}

			log.Printf("StreamingSource subscribed to topic %s", t)
			ns.subscriptions = append(ns.subscriptions, sub)
		}(topic)
	}

	// wait for an interrupt to unsubscribe topics
	go ns.awaitCleanup()

	select {
	case <-sigchan:
		log.Print("StreamingSource received termination signal, cleaning up...")
		ns.cancelCtx()
	case <-ns.ctx.Done():
	}

	ns.wg.Wait()
	close(ns.out)
	if err := ns.conn.Close(); err != nil {
		log.Printf("Failed to close NATS Streaming connection: %s", err)
	} else {
		log.Print("NATS Streaming connection closed")
	}
}

func (ns *StreamingSource) awaitCleanup() {
	ns.wg.Add(1)
	defer ns.wg.Done()

	select {
	case <-ns.ctx.Done():
		for _, sub := range ns.subscriptions {
			ns.wg.Add(1)
			go func(sub stan.Subscription) {
				defer ns.wg.Done()

				if err := sub.Unsubscribe(); err != nil {
					log.Fatal("Failed to remove NATS subscription")
				}
			}(sub)
		}
	default:
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
// Deprecated: Use JetStreamSink instead.
type StreamingSink struct {
	conn  stan.Conn
	topic string
	in    chan any
}

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

// init starts the stream processing loop.
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

	if err := ns.conn.Close(); err != nil {
		log.Printf("Failed to close NATS Streaming connection: %s", err)
	} else {
		log.Print("NATS Streaming connection closed")
	}
}

// In returns the input channel of the StreamingSink connector.
func (ns *StreamingSink) In() chan<- any {
	return ns.in
}
