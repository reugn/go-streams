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

	logger *slog.Logger
}

var _ streams.Source = (*Source)(nil)

// NewSource creates and returns a new [Source] using the default dialer.
func NewSource(ctx context.Context, url string, logger *slog.Logger) (*Source, error) {
	return NewSourceWithDialer(ctx, url, ws.DefaultDialer, logger)
}

// NewSourceWithDialer returns a new [Source] using the provided dialer.
func NewSourceWithDialer(ctx context.Context, url string,
	dialer *ws.Dialer, logger *slog.Logger) (*Source, error) {
	// create a new client connection
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
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

	// asynchronously consume data and send it downstream
	go source.process(ctx)

	return source, nil
}

func (s *Source) process(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			messageType, payload, err := s.connection.ReadMessage()
			if err != nil {
				s.logger.Error("Error in connection.ReadMessage",
					slog.Any("error", err))
			} else {
				// exit loop on CloseMessage
				if messageType == ws.CloseMessage {
					break loop
				}
				s.out <- Message{
					MsgType: messageType,
					Payload: payload,
				}
			}
		}
	}

	s.logger.Info("Closing connector")
	close(s.out)

	if err := s.connection.Close(); err != nil {
		s.logger.Warn("Error in connection.Close", slog.Any("error", err))
	}
}

// Via asynchronously streams data to the given Flow and returns it.
func (s *Source) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(s, operator)
	return operator
}

// Out returns the output channel of the Source connector.
func (s *Source) Out() <-chan any {
	return s.out
}

// Sink represents a WebSocket sink connector.
type Sink struct {
	connection *ws.Conn
	in         chan any

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*Sink)(nil)

// NewSink creates and returns a new [Sink] using the default dialer.
func NewSink(url string, logger *slog.Logger) (*Sink, error) {
	return NewSinkWithDialer(url, ws.DefaultDialer, logger)
}

// NewSinkWithDialer returns a new [Sink] using the provided dialer.
func NewSinkWithDialer(url string, dialer *ws.Dialer, logger *slog.Logger) (*Sink, error) {
	// create a new client connection
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
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
		done:       make(chan struct{}),
		logger:     logger,
	}

	// begin processing upstream data
	go sink.process()

	return sink, nil
}

func (s *Sink) process() {
	defer close(s.done) // signal data processing completion

	for msg := range s.in {
		var err error
		switch message := msg.(type) {
		case Message:
			err = s.connection.WriteMessage(message.MsgType, message.Payload)
		case *Message:
			err = s.connection.WriteMessage(message.MsgType, message.Payload)
		case string:
			err = s.connection.WriteMessage(ws.TextMessage, []byte(message))
		case []byte:
			err = s.connection.WriteMessage(ws.BinaryMessage, message)
		default:
			s.logger.Error("Unsupported message type",
				slog.String("type", fmt.Sprintf("%T", message)))
		}

		if err != nil {
			s.logger.Error("Error processing message", slog.Any("error", err))
		}
	}

	s.logger.Info("Closing connector")
	if err := s.connection.Close(); err != nil {
		s.logger.Warn("Error in connection.Close", slog.Any("error", err))
	}
}

// In returns the input channel of the Sink connector.
func (s *Sink) In() chan<- any {
	return s.in
}

// AwaitCompletion blocks until the Sink connector has completed
// processing all the received data.
func (s *Sink) AwaitCompletion() {
	<-s.done
}
