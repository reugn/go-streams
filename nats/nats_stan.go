package nats

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	stan "github.com/nats-io/stan.go"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/util"
)

// NatsSource connector
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

// NewNatsSource returns a new NatsSource instance
func NewNatsSource(ctx context.Context, conn stan.Conn, subscriptionType stan.SubscriptionOption, topics ...string) *NatsSource {
	cctx, cancel := context.WithCancel(ctx)
	out := make(chan interface{})

	src := NatsSource{
		conn,
		[]stan.Subscription{},
		subscriptionType,
		topics,
		out,
		cctx,
		cancel,
		&sync.WaitGroup{},
	}

	go src.init()

	return &src
}

func (ns *NatsSource) init() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Bind all topic subscribers
	for _, topic := range ns.topics {
		go func(t string) {
			ns.wg.Add(1)
			defer ns.wg.Done()
			sub, err := ns.conn.Subscribe(t, func(msg *stan.Msg) {
				ns.out <- msg
			}, ns.subscriptionType)
			util.Check(err)
			log.Println(fmt.Sprintf("NATS source subscribed to topic %s", t))
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
			go func(sub stan.Subscription) {
				ns.wg.Add(1)
				defer ns.wg.Done()
				err := sub.Unsubscribe()
				util.Check(err)
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

// NatsSink connector
type NatsSink struct {
	conn  stan.Conn
	topic string
	in    chan interface{}
}

// NewNatsSink returns a new NatsSink instance
func NewNatsSink(conn stan.Conn, topic string) *NatsSink {
	sink := NatsSink{
		conn,
		topic,
		make(chan interface{}),
	}

	go sink.init()

	return &sink
}

func (ns *NatsSink) init() {
	for msg := range ns.in {
		switch m := msg.(type) {
		case *stan.Msg:
			err := ns.conn.Publish(ns.topic, m.Data)
			util.Check(err)
		case []byte:
			err := ns.conn.Publish(ns.topic, m)
			util.Check(err)
		default:
			log.Printf("Unsupported NATS message publish type: %v", m)
		}
	}

	log.Printf("Closing the NATS sink connection")

	ns.conn.Close()
}

// In returns an input channel for receiving data
func (ns *NatsSink) In() chan<- interface{} {
	return ns.in
}
