package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// JetStreamSourceConfig specifies parameters for the JetStream source connector.
// Use NewJetStreamSourceConfig to create a new JetStreamSourceConfig with default values.
type JetStreamSourceConfig struct {
	Conn           *nats.Conn
	JetStreamCtx   nats.JetStreamContext
	Subject        string
	ConsumerName   string         // For an ephemeral pull consumer use an empty string.
	FetchBatchSize int            // FetchBatchSize is used by the pull consumer.
	Ack            bool           // Ack determines whether to acknowledge delivered messages by the consumer.
	SubOpts        []nats.SubOpt  // SubOpt configures options for subscribing to JetStream consumers.
	PullOpts       []nats.PullOpt // PullOpt are the options that can be passed when pulling a batch of messages.
	AckOpts        []nats.AckOpt  // AckOpt are the options that can be passed when acknowledge a message.
}

// validate validates the JetStream source configuration values.
func (config *JetStreamSourceConfig) validate() error {
	if config == nil {
		return errors.New("config is nil")
	}
	if config.Conn == nil {
		return errors.New("connection is nil")
	}
	if config.JetStreamCtx == nil {
		return errors.New("JetStream context is nil")
	}
	if config.Subject == "" {
		return errors.New("subject is empty")
	}
	if config.FetchBatchSize < 1 {
		return errors.New("nonpositive fetch batch size")
	}
	if config.SubOpts == nil {
		config.SubOpts = []nats.SubOpt{}
	}
	if config.PullOpts == nil {
		config.PullOpts = []nats.PullOpt{}
	}
	if config.AckOpts == nil {
		config.AckOpts = []nats.AckOpt{}
	}
	return nil
}

// NewJetStreamSourceConfig returns a new [JetStreamSourceConfig] with default values.
func NewJetStreamSourceConfig(conn *nats.Conn, jetStreamContext nats.JetStreamContext,
	subject string) *JetStreamSourceConfig {
	return &JetStreamSourceConfig{
		Conn:           conn,
		JetStreamCtx:   jetStreamContext,
		Subject:        subject,
		FetchBatchSize: 256,
		Ack:            true,
	}
}

// JetStreamSource represents a NATS JetStream source connector.
type JetStreamSource struct {
	config       *JetStreamSourceConfig
	subscription *nats.Subscription
	out          chan any
	logger       *slog.Logger
}

var _ streams.Source = (*JetStreamSource)(nil)

// NewJetStreamSource returns a new [JetStreamSource] connector.
// A pull-based subscription is used to consume data from the subject.
func NewJetStreamSource(ctx context.Context, config *JetStreamSourceConfig,
	logger *slog.Logger) (*JetStreamSource, error) {
	// create a pull based consumer
	subscription, err := config.JetStreamCtx.PullSubscribe(config.Subject,
		config.ConsumerName, config.SubOpts...)
	if err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "nats.jetstream"),
		slog.String("type", "source")))

	jetStreamSource := &JetStreamSource{
		config:       config,
		subscription: subscription,
		out:          make(chan any),
		logger:       logger,
	}
	go jetStreamSource.init(ctx)

	return jetStreamSource, nil
}

// init starts the stream processing loop.
func (js *JetStreamSource) init(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
		}
		// pull a batch of messages from the stream
		messages, err := js.subscription.Fetch(js.config.FetchBatchSize, js.config.PullOpts...)
		if err != nil {
			js.logger.Error("Error in subscription.Fetch", slog.Any("error", err))
			break loop
		}
		if len(messages) == 0 {
			js.logger.Debug("Message batch is empty")
			continue
		}
		for _, msg := range messages {
			// send the message downstream
			js.out <- msg
			if js.config.Ack {
				// acknowledge the message
				if err := msg.Ack(js.config.AckOpts...); err != nil {
					js.logger.Error("Failed to acknowledge message",
						slog.Any("error", err))
				}
			} else {
				// reset the redelivery timer on the server
				if err := msg.InProgress(js.config.AckOpts...); err != nil {
					js.logger.Error("Failed to set message in progress",
						slog.Any("error", err))
				}
			}
		}
	}

	if err := js.subscription.Drain(); err != nil {
		js.logger.Error("Failed to drain subscription",
			slog.Any("error", err))
	}
	js.logger.Info("Closing connector")
	close(js.out)
}

// Via streams data to a specified operator and returns it.
func (js *JetStreamSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(js, operator)
	return operator
}

// Out returns the output channel of the JetStreamSource connector.
func (js *JetStreamSource) Out() <-chan any {
	return js.out
}

// JetStreamSinkConfig specifies parameters for the JetStream sink connector.
type JetStreamSinkConfig struct {
	Conn         *nats.Conn
	JetStreamCtx nats.JetStreamContext
	Subject      string
	DrainConn    bool          // Determines whether to drain the connection when the upstream is closed.
	PubOpts      []nats.PubOpt // PubOpt configures options for publishing JetStream messages.
}

// validate validates the JetStream sink configuration values.
func (config *JetStreamSinkConfig) validate() error {
	if config == nil {
		return errors.New("config is nil")
	}
	if config.Conn == nil {
		return errors.New("connection is nil")
	}
	if config.JetStreamCtx == nil {
		return errors.New("JetStream context is nil")
	}
	if config.Subject == "" {
		return errors.New("subject is empty")
	}
	if config.PubOpts == nil {
		config.PubOpts = []nats.PubOpt{}
	}
	return nil
}

// JetStreamSink represents a NATS JetStream sink connector.
type JetStreamSink struct {
	config *JetStreamSinkConfig
	in     chan any
	logger *slog.Logger
}

var _ streams.Sink = (*JetStreamSink)(nil)

// NewJetStreamSink returns a new [JetStreamSink] connector.
// The stream for the configured subject is expected to exist.
func NewJetStreamSink(config *JetStreamSinkConfig,
	logger *slog.Logger) (*JetStreamSink, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "nats.jetstream"),
		slog.String("type", "sink")))

	jetStreamSink := &JetStreamSink{
		config: config,
		in:     make(chan any),
		logger: logger,
	}
	go jetStreamSink.init()

	return jetStreamSink, nil
}

// init starts the stream processing loop.
func (js *JetStreamSink) init() {
	for msg := range js.in {
		var err error
		switch message := msg.(type) {
		case *nats.Msg:
			_, err = js.config.JetStreamCtx.Publish(
				js.config.Subject,
				message.Data,
				js.config.PubOpts...)
		case []byte:
			_, err = js.config.JetStreamCtx.Publish(
				js.config.Subject,
				message,
				js.config.PubOpts...)
		default:
			js.logger.Error("Unsupported message type",
				slog.String("type", fmt.Sprintf("%T", message)))
		}

		if err != nil {
			js.logger.Error("Error processing message",
				slog.Any("error", err))
		}
	}

	if js.config.DrainConn {
		// puts all subscriptions into a drain state
		if err := js.config.Conn.Drain(); err != nil {
			js.logger.Error("Failed to drain connection",
				slog.Any("error", err))
		}
	}
	js.logger.Info("Closing connector")
}

// In returns the input channel of the JetStreamSink connector.
func (js *JetStreamSink) In() chan<- any {
	return js.in
}
