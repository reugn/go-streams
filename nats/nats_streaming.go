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
	out       chan interface{}
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        *sync.WaitGroup
}

// NewStreamingSource returns a new StreamingSource instance.
func NewStreamingSource(ctx context.Context, conn stan.Conn, subscriptionType stan.SubscriptionOption,
	topics ...string) *StreamingSource {
	cctx, cancel := context.WithCancel(ctx)

	streamingSource := &StreamingSource{
		conn:             conn,
		subscriptions:    []stan.Subscription{},
		subscriptionType: subscriptionType,
		topics:           topics,
		out:              make(chan interface{}),
		ctx:              cctx,
		cancelCtx:        cancel,
		wg:               &sync.WaitGroup{},
	}

	go streamingSource.init()
	return streamingSource
}

func (ns *StreamingSource) init() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Bind all topic subscribers
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

	// Wait for an interrupt to unsubscribe topics
	go ns.awaitCleanup()

	select {
	case <-sigchan:
		log.Println("StreamingSource received termination signal, cleaning up...")
		ns.cancelCtx()
	case <-ns.ctx.Done():
	}

	ns.wg.Wait()

	close(ns.out)
	ns.conn.Close()
	log.Println("StreamingSource cleanup complete")
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

// Via streams data through a given flow
func (ns *StreamingSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ns, _flow)
	return _flow
}

// Out returns the output channel for the data
func (ns *StreamingSource) Out() <-chan interface{} {
	return ns.out
}

// StreamingSink represents a NATS Streaming sink connector.
// Deprecated: Use JetStreamSink instead.
type StreamingSink struct {
	conn  stan.Conn
	topic string
	in    chan interface{}
}

// NewStreamingSink returns a new StreamingSink instance.
func NewStreamingSink(conn stan.Conn, topic string) *StreamingSink {
	streamingSink := &StreamingSink{
		conn:  conn,
		topic: topic,
		in:    make(chan interface{}),
	}

	go streamingSink.init()
	return streamingSink
}

func (ns *StreamingSink) init() {
	for msg := range ns.in {
		var err error
		switch m := msg.(type) {
		case *stan.Msg:
			err = ns.conn.Publish(ns.topic, m.Data)

		case []byte:
			err = ns.conn.Publish(ns.topic, m)

		default:
			log.Printf("Unsupported NATS message type: %v", m)
		}

		if err != nil {
			log.Printf("Error processing NATS message: %s", err)
		}
	}

	log.Printf("Closing NATS sink connection")
	ns.conn.Close()
}

// In returns an input channel for receiving data
func (ns *StreamingSink) In() chan<- interface{} {
	return ns.in
}
