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

// NatsSource represents a NATS Streaming source connector.
type NatsSource struct {
	conn             stan.Conn
	subscriptions    []stan.Subscription
	subscriptionType stan.SubscriptionOption

	topics    []string
	out       chan interface{}
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        *sync.WaitGroup
}

// NewNatsSource returns a new NatsSource instance.
func NewNatsSource(ctx context.Context, conn stan.Conn, subscriptionType stan.SubscriptionOption,
	topics ...string) *NatsSource {
	cctx, cancel := context.WithCancel(ctx)

	natsSource := NatsSource{
		conn:             conn,
		subscriptions:    []stan.Subscription{},
		subscriptionType: subscriptionType,
		topics:           topics,
		out:              make(chan interface{}),
		ctx:              cctx,
		cancelCtx:        cancel,
		wg:               &sync.WaitGroup{},
	}

	go natsSource.init()
	return &natsSource
}

func (ns *NatsSource) init() {
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

			log.Printf("NATS source subscribed to topic %s", t)
			ns.subscriptions = append(ns.subscriptions, sub)
		}(topic)
	}

	// Wait for an interrupt to unsubscribe topics
	go ns.awaitCleanup()

	select {
	case <-sigchan:
		log.Println("NATS source received termination signal, cleaning up...")
		ns.cancelCtx()
	case <-ns.ctx.Done():
	}

	ns.wg.Wait()

	close(ns.out)
	ns.conn.Close()
	log.Println("NATS source cleanup complete")
}

func (ns *NatsSource) awaitCleanup() {
	ns.wg.Add(1)
	defer ns.wg.Done()

	select {
	case <-ns.ctx.Done():
		for _, s := range ns.subscriptions {
			ns.wg.Add(1)
			go func(sub stan.Subscription) {
				defer ns.wg.Done()

				if err := sub.Unsubscribe(); err != nil {
					log.Fatal("Failed to remove NATS subscription")
				}
			}(s)
		}
	default:
	}
}

// Via streams data through a given flow
func (ns *NatsSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ns, _flow)
	return _flow
}

// Out returns the output channel for the data
func (ns *NatsSource) Out() <-chan interface{} {
	return ns.out
}

// NatsSink represents a NATS Streaming sink connector.
type NatsSink struct {
	conn  stan.Conn
	topic string
	in    chan interface{}
}

// NewNatsSink returns a new NatsSink instance
func NewNatsSink(conn stan.Conn, topic string) *NatsSink {
	natsSink := NatsSink{
		conn:  conn,
		topic: topic,
		in:    make(chan interface{}),
	}

	go natsSink.init()
	return &natsSink
}

func (ns *NatsSink) init() {
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
func (ns *NatsSink) In() chan<- interface{} {
	return ns.in
}
