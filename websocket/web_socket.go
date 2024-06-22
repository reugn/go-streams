package websocket

import (
	"context"
	"log"

	ws "github.com/gorilla/websocket"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// Message represents a WebSocket message container.
// Message types are defined in [RFC 6455], section 11.8.
//
// [RFC 6455]: https://www.rfc-editor.org/rfc/rfc6455.html#section-11.8
type Message struct {
	MsgType int
	Payload []byte
}

// Source represents a WebSocket source connector.
type Source struct {
	connection *ws.Conn
	out        chan any
}

var _ streams.Source = (*Source)(nil)

// NewSource creates and returns a new Source using the default dialer.
func NewSource(ctx context.Context, url string) (*Source, error) {
	return NewSourceWithDialer(ctx, url, ws.DefaultDialer)
}

// NewSourceWithDialer returns a new Source using the specified dialer.
func NewSourceWithDialer(ctx context.Context, url string,
	dialer *ws.Dialer) (*Source, error) {
	// create a new client connection
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	source := &Source{
		connection: conn,
		out:        make(chan any),
	}
	go source.init(ctx)

	return source, nil
}

func (wsock *Source) init(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			messageType, payload, err := wsock.connection.ReadMessage()
			if err != nil {
				log.Printf("Error in ReadMessage: %s", err)
			} else {
				// exit loop on CloseMessage
				if messageType == ws.CloseMessage {
					break loop
				}
				wsock.out <- Message{
					MsgType: messageType,
					Payload: payload,
				}
			}
		}
	}
	log.Print("Closing WebSocket source connector")
	close(wsock.out)
	if err := wsock.connection.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// Via streams data to a specified operator and returns it.
func (wsock *Source) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(wsock, operator)
	return operator
}

// Out returns the output channel of the Source connector.
func (wsock *Source) Out() <-chan any {
	return wsock.out
}

// Sink represents a WebSocket sink connector.
type Sink struct {
	connection *ws.Conn
	in         chan any
}

var _ streams.Sink = (*Sink)(nil)

// NewSink creates and returns a new Sink using the default dialer.
func NewSink(url string) (*Sink, error) {
	return NewSinkWithDialer(url, ws.DefaultDialer)
}

// NewSinkWithDialer returns a new Sink using the specified dialer.
func NewSinkWithDialer(url string, dialer *ws.Dialer) (*Sink, error) {
	// create a new client connection
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	sink := &Sink{
		connection: conn,
		in:         make(chan any),
	}
	go sink.init()

	return sink, nil
}

func (wsock *Sink) init() {
	for msg := range wsock.in {
		var err error
		switch m := msg.(type) {
		case Message:
			err = wsock.connection.WriteMessage(m.MsgType, m.Payload)
		case *Message:
			err = wsock.connection.WriteMessage(m.MsgType, m.Payload)
		case string:
			err = wsock.connection.WriteMessage(ws.TextMessage, []byte(m))
		case []byte:
			err = wsock.connection.WriteMessage(ws.BinaryMessage, m)
		default:
			log.Printf("Unsupported message type: %T", m)
		}

		if err != nil {
			log.Printf("Error processing WebSocket message: %s", err)
		}
	}
	log.Print("Closing WebSocket sink connector")
	if err := wsock.connection.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// In returns the input channel of the Sink connector.
func (wsock *Sink) In() chan<- any {
	return wsock.in
}
