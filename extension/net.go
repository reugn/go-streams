package extension

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ConnType represents a network connection type.
type ConnType string

const (
	// TCP connection type.
	TCP ConnType = "tcp"
	// UDP connection type.
	UDP ConnType = "udp"
)

// NetSource represents an inbound network socket connector.
type NetSource struct {
	ctx      context.Context
	conn     net.Conn
	listener net.Listener
	connType ConnType
	out      chan any
}

var _ streams.Source = (*NetSource)(nil)

// NewNetSource returns a new NetSource connector.
func NewNetSource(ctx context.Context, connType ConnType, address string) (*NetSource, error) {
	var (
		conn     net.Conn
		listener net.Listener
	)

	out := make(chan any)
	switch connType {
	case TCP:
		addr, err := net.ResolveTCPAddr(string(connType), address)
		if err != nil {
			return nil, fmt.Errorf("failed to ResolveTCPAddr: %w", err)
		}

		listener, err = net.ListenTCP(string(connType), addr)
		if err != nil {
			return nil, fmt.Errorf("failed to ListenTCP: %w", err)
		}

		go acceptConnections(listener, out)
	case UDP:
		addr, err := net.ResolveUDPAddr(string(connType), address)
		if err != nil {
			return nil, fmt.Errorf("failed to ResolveUDPAddr: %w", err)
		}

		conn, err = net.ListenUDP(string(connType), addr)
		if err != nil {
			return nil, fmt.Errorf("failed to ListenUDP: %w", err)
		}

		go handleConnection(conn, out)
	default:
		return nil, fmt.Errorf("invalid connection type: %s", connType)
	}

	netSource := &NetSource{
		ctx:      ctx,
		conn:     conn,
		listener: listener,
		connType: connType,
		out:      out,
	}

	// start a goroutine to await the context cancellation and then
	// shut down the network source
	go netSource.awaitShutdown()

	return netSource, nil
}

func (ns *NetSource) awaitShutdown() {
	<-ns.ctx.Done()

	if ns.conn != nil {
		_ = ns.conn.Close()
	}

	if ns.listener != nil {
		_ = ns.listener.Close()
	}

	close(ns.out)
}

// acceptConnections accepts new TCP connections.
func acceptConnections(listener net.Listener, out chan<- any) {
	for {
		// block and return the next connection to the listener
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("listener.Accept() failed with: %s", err)
			return
		}

		// handle the new connection
		go handleConnection(conn, out)
	}
}

// handleConnection manages a single network connection, reading newline-delimited data
// from it and sending it to the provided output channel.
func handleConnection(conn net.Conn, out chan<- any) {
	log.Printf("NetSource connected on: %v", conn.LocalAddr())
	reader := bufio.NewReader(conn)

	for {
		bufferBytes, err := reader.ReadBytes('\n')
		if len(bufferBytes) > 0 {
			out <- string(bufferBytes)
		}

		if err != nil {
			log.Printf("handleConnection failed with: %s", err)
			break
		}
	}

	log.Printf("Closing the NetSource connection %v", conn.LocalAddr())
	if err := conn.Close(); err != nil {
		log.Printf("Failed to close connection %v", conn.LocalAddr())
	}
}

// Via asynchronously streams data to the given Flow and returns it.
func (ns *NetSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ns, operator)
	return operator
}

// Out returns the output channel of the NetSource connector.
func (ns *NetSource) Out() <-chan any {
	return ns.out
}

// NetSink represents an outbound network socket connector.
type NetSink struct {
	conn     net.Conn
	connType ConnType
	in       chan any
	done     chan struct{}
}

var _ streams.Sink = (*NetSink)(nil)

// NewNetSink returns a new NetSink connector.
func NewNetSink(connType ConnType, address string) (*NetSink, error) {
	conn, err := net.DialTimeout(string(connType), address, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	log.Printf("NetSink connected on: %v", conn.LocalAddr())

	netSink := &NetSink{
		conn:     conn,
		connType: connType,
		in:       make(chan any),
		done:     make(chan struct{}),
	}

	// asynchronously process stream data
	go netSink.process()

	return netSink, nil
}

func (ns *NetSink) process() {
	defer close(ns.done)

	for msg := range ns.in {
		switch message := msg.(type) {
		case string:
			if _, err := ns.conn.Write([]byte(message)); err != nil {
				log.Printf("NetSink failed to write to connection %v: %v",
					ns.conn.LocalAddr(), err)
			}
		default:
			log.Printf("NetSink unsupported message type: %T", message)
		}
	}

	log.Printf("Closing the NetSink connection %v", ns.conn.LocalAddr())
	if err := ns.conn.Close(); err != nil {
		log.Printf("Failed to close connection %v", ns.conn.LocalAddr())
	}
}

// In returns the input channel of the NetSink connector.
func (ns *NetSink) In() chan<- any {
	return ns.in
}

// AwaitCompletion blocks until the NetSink has processed all received data,
// closed the connection, and released all resources.
func (ns *NetSink) AwaitCompletion() {
	<-ns.done
}
