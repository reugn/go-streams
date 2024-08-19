package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// SaramaSource represents an Apache Kafka source connector.
type SaramaSource struct {
	consumer sarama.ConsumerGroup
	handler  sarama.ConsumerGroupHandler
	topics   []string
	out      chan any
	logger   *slog.Logger
}

var _ streams.Source = (*SaramaSource)(nil)

// NewSaramaSource returns a new [SaramaSource] connector.
func NewSaramaSource(ctx context.Context, consumerGroup sarama.ConsumerGroup,
	topics []string, logger *slog.Logger) *SaramaSource {

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "kafka.sarama"),
		slog.String("type", "source")))

	out := make(chan any)
	handler := &groupHandler{
		ready:  make(chan struct{}),
		out:    out,
		logger: logger,
	}

	source := &SaramaSource{
		consumer: consumerGroup,
		handler:  handler,
		topics:   topics,
		out:      out,
		logger:   logger,
	}
	go source.init(ctx)

	return source
}

func (ks *SaramaSource) init(ctx context.Context) {
loop:
	for {
		handler := ks.handler.(*groupHandler)
		// Consume is called inside an infinite loop, so that when a
		// server-side rebalance happens, the consumer session will be
		// recreated to get the new claims.
		if err := ks.consumer.Consume(ctx, ks.topics, handler); err != nil {
			ks.logger.Error("Error in consumer.Consume", slog.Any("error", err))
		}
		handler.ready = make(chan struct{})

		select {
		case <-ctx.Done():
			break loop
		default:
		}
	}
	ks.logger.Info("Closing connector")
	close(ks.out)
	if err := ks.consumer.Close(); err != nil {
		ks.logger.Warn("Error in consumer.Close", slog.Any("error", err))
	}
}

// Via streams data to a specified operator and returns it.
func (ks *SaramaSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ks, operator)
	return operator
}

// Out returns the output channel of the KafkaSource connector.
func (ks *SaramaSource) Out() <-chan any {
	return ks.out
}

// groupHandler implements the [sarama.ConsumerGroupHandler] interface.
// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for the consumer group session life-cycle and
// allows for triggering logic before or after the consume loop(s).
type groupHandler struct {
	ready  chan struct{}
	out    chan any
	logger *slog.Logger
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (handler *groupHandler) Setup(sarama.ConsumerGroupSession) error {
	// mark the consumer as ready
	close(handler.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (handler *groupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (handler *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				handler.logger.Debug("Message claimed",
					slog.String("value", string(message.Value)),
					slog.Any("timestamp", message.Timestamp),
					slog.String("topic", message.Topic))
				session.MarkMessage(message, "") // mark the message as consumed
				handler.out <- message
			}

		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

// SaramaSink represents an Apache Kafka sink connector.
type SaramaSink struct {
	producer sarama.SyncProducer
	topic    string
	in       chan any
	logger   *slog.Logger
}

var _ streams.Sink = (*SaramaSink)(nil)

// NewSaramaSink returns a new [SaramaSink] connector.
func NewSaramaSink(syncProducer sarama.SyncProducer, topic string,
	logger *slog.Logger) *SaramaSink {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "kafka.sarama"),
		slog.String("type", "sink")))

	sink := &SaramaSink{
		producer: syncProducer,
		topic:    topic,
		in:       make(chan any),
		logger:   logger,
	}
	go sink.init()

	return sink
}

func (ks *SaramaSink) init() {
	for msg := range ks.in {
		var err error
		switch message := msg.(type) {
		case *sarama.ProducerMessage:
			_, _, err = ks.producer.SendMessage(message)
		case *sarama.ConsumerMessage:
			producerMessage := &sarama.ProducerMessage{
				Topic: ks.topic,
				Key:   sarama.StringEncoder(message.Key),
				Value: sarama.StringEncoder(message.Value),
			}
			_, _, err = ks.producer.SendMessage(producerMessage)
		case string:
			producerMessage := &sarama.ProducerMessage{
				Topic: ks.topic,
				Value: sarama.StringEncoder(message),
			}
			_, _, err = ks.producer.SendMessage(producerMessage)
		default:
			ks.logger.Error("Unsupported message type",
				slog.String("type", fmt.Sprintf("%T", message)))
		}

		if err != nil {
			ks.logger.Error("Error processing message", slog.Any("error", err))
		}
	}
	ks.logger.Info("Closing connector")
	if err := ks.producer.Close(); err != nil {
		ks.logger.Warn("Error in producer.Close", slog.Any("error", err))
	}
}

// In returns the input channel of the SaramaSink connector.
func (ks *SaramaSink) In() chan<- any {
	return ks.in
}
