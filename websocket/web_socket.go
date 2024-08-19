package websocket

import (
	"context"
	"fmt"
	"log/slog"

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
	logger     *slog.Logger
}

var _ streams.Source = (*Source)(nil)

// NewSource creates and returns a new [Source] using the default dialer.
func NewSource(ctx context.Context, url string, logger *slog.Logger) (*Source, error) {
	return NewSourceWithDialer(ctx, url, ws.DefaultDialer, logger)
}

// NewSourceWithDialer returns a new [Source] using the specified dialer.
func NewSourceWithDialer(ctx context.Context, url string,
	dialer *ws.Dialer, logger *slog.Logger) (*Source, error) {
	// create a new client connection
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "websocket"),
		slog.String("type", "source")))

	source := &Source{
		connection: conn,
		out:        make(chan any),
		logger:     logger,
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
				wsock.logger.Error("Error in connection.ReadMessage",
					slog.Any("error", err))
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
	wsock.logger.Info("Closing connector")
	close(wsock.out)
	if err := wsock.connection.Close(); err != nil {
		wsock.logger.Warn("Error in connection.Close", slog.Any("error", err))
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
	logger     *slog.Logger
}

var _ streams.Sink = (*Sink)(nil)

// NewSink creates and returns a new [Sink] using the default dialer.
func NewSink(url string, logger *slog.Logger) (*Sink, error) {
	return NewSinkWithDialer(url, ws.DefaultDialer, logger)
}

// NewSinkWithDialer returns a new [Sink] using the specified dialer.
func NewSinkWithDialer(url string, dialer *ws.Dialer, logger *slog.Logger) (*Sink, error) {
	// create a new client connection
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "websocket"),
		slog.String("type", "sink")))

	sink := &Sink{
		connection: conn,
		in:         make(chan any),
		logger:     logger,
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
			wsock.logger.Error("Unsupported message type",
				slog.String("type", fmt.Sprintf("%T", m)))
		}

		if err != nil {
			wsock.logger.Error("Error processing message",
				slog.Any("error", err))
		}
	}
	wsock.logger.Info("Closing connector")
	if err := wsock.connection.Close(); err != nil {
		wsock.logger.Warn("Error in connection.Close", slog.Any("error", err))
	}
}

// In returns the input channel of the Sink connector.
func (wsock *Sink) In() chan<- any {
	return wsock.in
}
