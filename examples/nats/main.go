package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	ext "github.com/reugn/go-streams/nats"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 {
		streaming()
	} else {
		jetStream()
	}
}

// docker run --rm --name nats-js -p 4222:4222 nats -js
//
//nolint:funlen
func jetStream() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// connect to the NATS server
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}

	// create JetStreamContext
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	streamName := "stream1"
	subjectName := "stream1.subject1"

	// check if the stream already exists; if not, create it
	stream, _ := js.StreamInfo(streamName)
	if stream == nil {
		// create stream
		// for the set of stream configuration options, see:
		// https://docs.nats.io/nats-concepts/jetstream/streams#configuration
		_, err = js.AddStream(&nats.StreamConfig{
			Name:                 streamName,
			Subjects:             []string{subjectName},
			DiscardNewPerSubject: true, // exactly-once semantics
			MaxMsgsPerSubject:    1024,
			Discard:              nats.DiscardNew,
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Stream %s has been created", streamName)
	}

	// create a new JetStream source connector
	sourceConfig := &ext.JetStreamSourceConfig{
		Conn:           nc,
		JetStreamCtx:   js,
		Subject:        subjectName,
		ConsumerName:   "JetStreamSource",
		FetchBatchSize: 64,
		Ack:            true,
		SubOpts: []nats.SubOpt{
			nats.PullMaxWaiting(128),
		},
		PullOpts: []nats.PullOpt{
			nats.Context(ctx), // sets deadline for fetch
		},
	}
	jetSource, err := ext.NewJetStreamSource(ctx, sourceConfig, nil)
	if err != nil {
		log.Fatal(err)
	}

	fileSource := extension.NewFileSource("in.txt")
	toUpperMapFlow := flow.NewMap(toUpperString, 1)

	// create a new JetStream sink connector
	sinkConfig := &ext.JetStreamSinkConfig{
		Conn:         nc,
		JetStreamCtx: js,
		Subject:      subjectName,
		PubOpts:      []nats.PubOpt{nats.Context(ctx)},
	}
	jetSink, err := ext.NewJetStreamSink(sinkConfig, nil)
	if err != nil {
		log.Fatal(err)
	}

	fetchJetMsgMapFlow := flow.NewMap(fetchJetMsg, 1)
	stdOutSink := extension.NewStdoutSink()

	fileSource.
		Via(toUpperMapFlow).
		To(jetSink)

	jetSource.
		Via(fetchJetMsgMapFlow).
		To(stdOutSink)
}

// docker run --rm --name nats-streaming -p 4223:4223 -p 8223:8223 nats-streaming -p 4223 -m 8223
func streaming() {
	ctx := context.Background()

	fileSource := extension.NewFileSource("in.txt")
	toUpperMapFlow := flow.NewMap(toUpperString, 1)
	prodConn, err := stan.Connect("test-cluster", "test-producer",
		stan.NatsURL("nats://localhost:4223"))
	if err != nil {
		log.Fatal(err)
	}
	streamingSink := ext.NewStreamingSink(prodConn, "topic1", nil)

	subConn, err := stan.Connect("test-cluster", "test-subscriber",
		stan.NatsURL("nats://localhost:4223"))
	if err != nil {
		log.Fatal(err)
	}
	// This example uses the StartWithLastReceived subscription option
	// there are more available at https://docs.nats.io/developing-with-nats-streaming/receiving
	streamingSource := ext.NewStreamingSource(ctx, subConn, stan.StartWithLastReceived(),
		[]string{"topic1"}, nil)
	fetchStanMsgMapFlow := flow.NewMap(fetchStanMsg, 1)
	stdOutSink := extension.NewStdoutSink()

	fileSource.
		Via(toUpperMapFlow).
		To(streamingSink)

	streamingSource.
		Via(fetchStanMsgMapFlow).
		To(stdOutSink)
}

var toUpperString = func(msg string) []byte {
	return []byte(strings.ReplaceAll(strings.ToUpper(msg), "\n", ""))
}

var fetchJetMsg = func(msg *nats.Msg) string {
	return string(msg.Data)
}

var fetchStanMsg = func(msg *stan.Msg) string {
	return string(msg.Data)
}
