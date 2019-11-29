package ext

import (
	"bufio"
	"errors"
	"log"
	"net"
	"time"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ConnType connection type
type ConnType string

const (
	// TCP connection type
	TCP ConnType = "tcp"
	// UDP connection type
	UDP ConnType = "udp"
)

// NetSource network socket connector
type NetSource struct {
	conn     net.Conn
	listener net.Listener
	connType ConnType
	out      chan interface{}
}

// NewNetSource creates a new NetSource
func NewNetSource(connType ConnType, address string) (*NetSource, error) {
	var err error
	var conn net.Conn
	var listener net.Listener
	out := make(chan interface{})

	switch connType {
	case TCP:
		addr, _ := net.ResolveTCPAddr(string(connType), address)
		listener, err = net.ListenTCP(string(connType), addr)

		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		go acceptConnections(listener, out)

	case UDP:
		addr, _ := net.ResolveUDPAddr(string(connType), address)
		conn, err = net.ListenUDP(string(connType), addr)

		if err != nil {
			log.Fatal(err)
			return nil, err
		}

		go handleConnection(conn, out)

	default:
		return nil, errors.New("Invalid connection type")
	}

	source := &NetSource{
		conn:     conn,
		listener: listener,
		connType: connType,
		out:      out,
	}

	return source, nil
}

// TCP Accept routine
func acceptConnections(listener net.Listener, out chan<- interface{}) {
	for {
		// accept new connection
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		// handle new connection
		go handleConnection(conn, out)
	}
	log.Printf("Closing NetSource TCP listener %v", listener)
	listener.Close()
}

// handleConnection routine
func handleConnection(conn net.Conn, out chan<- interface{}) {
	log.Printf("NetSource connected on: %v", conn.LocalAddr())
	reader := bufio.NewReader(conn)
	for {
		bufferBytes, err := reader.ReadBytes('\n')
		if err != nil {
			log.Fatal(err)
			break
		} else {
			out <- string(bufferBytes)
		}
	}
	log.Printf("Closing NetSource connection %v", conn)
	conn.Close()
}

// Via streams data through given flow
func (ns *NetSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ns, _flow)
	return _flow
}

// Out returns channel for sending data
func (ns *NetSource) Out() <-chan interface{} {
	return ns.out
}

// NetSink downstreams input events to a network soket
type NetSink struct {
	conn     net.Conn
	connType ConnType
	in       chan interface{}
}

// NewNetSink creates a new NetSink
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
		in:       make(chan interface{}),
	}

	go sink.init()
	return sink, nil
}

// start main loop
func (ns *NetSink) init() {
	log.Printf("NetSink connected on: %v", ns.conn.LocalAddr())
	writer := bufio.NewWriter(ns.conn)
	for msg := range ns.in {
		switch m := msg.(type) {
		case string:
			_, err := writer.WriteString(m)
			if err == nil {
				err = writer.Flush()
			}
		}
	}
	log.Printf("Closing NetSink connection %v", ns.conn)
	ns.conn.Close()
}

// In returns channel for receiving data
func (ns *NetSink) In() chan<- interface{} {
	return ns.in
}
