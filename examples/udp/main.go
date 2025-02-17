package main

import (
	"context"
	"io"
	"log"
	"math"
	"net"
	"strings"
	"time"

	ext "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

// This example demonstrates stream processing from a UDP source to a UDP
// sink, using the standard io.Reader and io.Writer connectors.
//
//	Test producer: nc -u 127.0.0.1 3434
//	Test consumer: nc -u -l 3535
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:3434")
	if err != nil {
		log.Fatalf("Error resolving UDP address: %v", err)
	}
	sourceConn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("Error listening on UDP: %v", err)
	}

	// define a buffer to hold incoming UDP packets
	// math.MaxUint16 is the maximum UDP packet size
	p := make([]byte, math.MaxUint16)

	// create a ReaderSource from the UDP connection
	source, err := ext.NewReaderSource(sourceConn, func(reader io.Reader) ([]byte, error) {
		n, err := reader.Read(p)
		return p[:n], err
	}, ext.WithContext(ctx))
	if err != nil {
		log.Fatal(err)
	}

	sinkConn, err := net.DialTimeout("udp", "127.0.0.1:3535", 10*time.Second)
	if err != nil {
		log.Fatalf("Error dialing UDP: %v", err)
	}

	// create a WriterSink to write data to the UDP connection
	sink, err := ext.NewWriterSink(sinkConn, ext.WithContextCancel(cancel))
	if err != nil {
		log.Fatal(err)
	}

	toUpperMapFlow := flow.NewMap(toUpper, 1)

	source.
		Via(toUpperMapFlow).
		To(sink)
}

var toUpper = func(msg []byte) string {
	log.Printf("Received: %s", msg)
	return strings.ToUpper(string(msg))
}
