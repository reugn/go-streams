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
	ext "github.com/reugn/go-streams/nats"

	"github.com/reugn/go-streams/flow"
)

func main() {
	args := os.Args[1:]
	runJet := true
	if len(args) > 0 {
		runJet = false
	}

	if runJet {
		jetStream()
	} else {
		streaming()
	}
}

// docker run --rm --name nats-js -p 4222:4222 nats -js
func jetStream() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fileSource := extension.NewFileSource("in.txt")
	toUpperMapFlow := flow.NewMap(toUpperString, 1)
	jetSink, err := ext.NewJetStreamSink("stream1", "stream1.subject1", "nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}

	jetSource, err := ext.NewJetStreamSource(ctx, "stream1.subject1", "nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	fetchJetMsgMapFlow := flow.NewMap(fetchJetMsg, 1)
	stdOutSInk := extension.NewStdoutSink()

	fileSource.
		Via(toUpperMapFlow).
		To(jetSink)

	jetSource.
		Via(fetchJetMsgMapFlow).
		To(stdOutSInk)
}

// docker run --rm --name nats-streaming -p 4223:4223 -p 8223:8223 nats-streaming -p 4223 -m 8223
func streaming() {
	ctx := context.Background()

	fileSource := extension.NewFileSource("in.txt")
	toUpperMapFlow := flow.NewMap(toUpperString, 1)
	prodConn, err := stan.Connect("test-cluster", "test-producer", stan.NatsURL("nats://localhost:4223"))
	if err != nil {
		log.Fatal(err)
	}
	streamingSink := ext.NewStreamingSink(prodConn, "topic1")

	subConn, err := stan.Connect("test-cluster", "test-subscriber", stan.NatsURL("nats://localhost:4223"))
	if err != nil {
		log.Fatal(err)
	}
	// This example uses the StartWithLastReceived subscription option
	// there are more available at https://docs.nats.io/developing-with-nats-streaming/receiving
	streamingSource := ext.NewStreamingSource(ctx, subConn, stan.StartWithLastReceived(), "topic1")
	fetchStanMsgMapFlow := flow.NewMap(fetchStanMsg, 1)
	stdOutSInk := extension.NewStdoutSink()

	fileSource.
		Via(toUpperMapFlow).
		To(streamingSink)

	streamingSource.
		Via(fetchStanMsgMapFlow).
		To(stdOutSInk)
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
