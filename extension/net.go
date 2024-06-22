package extension

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net"
	"time"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ConnType represents a connection type.
type ConnType string

const (
	// TCP connection type
	TCP ConnType = "tcp"

	// UDP connection type
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
	var err error
	var conn net.Conn
	var listener net.Listener
	out := make(chan any)

	switch connType {
	case TCP:
		addr, _ := net.ResolveTCPAddr(string(connType), address)
		listener, err = net.ListenTCP(string(connType), addr)
		if err != nil {
			return nil, err
		}
		go acceptConnections(listener, out)
	case UDP:
		addr, _ := net.ResolveUDPAddr(string(connType), address)
		conn, err = net.ListenUDP(string(connType), addr)
		if err != nil {
			return nil, err
		}
		go handleConnection(conn, out)
	default:
		return nil, errors.New("invalid connection type")
	}

	source := &NetSource{
		ctx:      ctx,
		conn:     conn,
		listener: listener,
		connType: connType,
		out:      out,
	}

	go source.listenCtx()
	return source, nil
}

func (ns *NetSource) listenCtx() {
	<-ns.ctx.Done()

	if ns.conn != nil {
		ns.conn.Close()
	}

	if ns.listener != nil {
		ns.listener.Close()
	}

	close(ns.out)
}

// acceptConnections accepts new TCP connections.
func acceptConnections(listener net.Listener, out chan<- any) {
	for {
		// accept a new connection
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("listener.Accept() failed with: %s", err)
			return
		}

		// handle the new connection
		go handleConnection(conn, out)
	}
}

// handleConnection handles new connections.
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

	log.Printf("Closing the NetSource connection %v", conn)
	conn.Close()
}

// Via streams data to a specified operator and returns it.
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
}

var _ streams.Sink = (*NetSink)(nil)

// NewNetSink returns a new NetSink connector.
func NewNetSink(connType ConnType, address string) (*NetSink, error) {
	var err error
	var conn net.Conn

	conn, err = net.DialTimeout(string(connType), address, time.Second*10)
	if err != nil {
		return nil, err
	}

	sink := &NetSink{
		conn:     conn,
		connType: connType,
		in:       make(chan any),
	}

	go sink.init()
	return sink, nil
}

// init starts the stream processing loop
func (ns *NetSink) init() {
	log.Printf("NetSink connected on: %v", ns.conn.LocalAddr())
	writer := bufio.NewWriter(ns.conn)

	for msg := range ns.in {
		switch m := msg.(type) {
		case string:
			_, err := writer.WriteString(m)
			if err == nil {
				writer.Flush()
			}
		default:
			log.Printf("NetSink unsupported message type: %T", m)
		}
	}

	log.Printf("Closing the NetSink connection %v", ns.conn)
	ns.conn.Close()
}

// In returns the input channel of the NetSink connector.
func (ns *NetSink) In() chan<- any {
	return ns.in
}
