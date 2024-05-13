package kafka

import (
	"context"
	"log"

	"github.com/IBM/sarama"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// KafkaSource represents an Apache Kafka source connector.
type KafkaSource struct {
	consumer sarama.ConsumerGroup
	handler  sarama.ConsumerGroupHandler
	topics   []string
	out      chan any
}

var _ streams.Source = (*KafkaSource)(nil)

// NewKafkaSource returns a new KafkaSource connector.
func NewKafkaSource(ctx context.Context, addrs []string, groupID string,
	config *sarama.Config, topics ...string) (*KafkaSource, error) {
	consumerGroup, err := sarama.NewConsumerGroup(addrs, groupID, config)
	if err != nil {
		return nil, err
	}

	out := make(chan any)
	source := &KafkaSource{
		consumer: consumerGroup,
		handler:  &GroupHandler{make(chan struct{}), out},
		topics:   topics,
		out:      out,
	}
	go source.init(ctx)

	return source, nil
}

func (ks *KafkaSource) init(ctx context.Context) {
loop:
	for {
		handler := ks.handler.(*GroupHandler)
		// Consume is called inside an infinite loop, so that when a
		// server-side rebalance happens, the consumer session will be
		// recreated to get the new claims.
		if err := ks.consumer.Consume(ctx, ks.topics, handler); err != nil {
			log.Printf("Error is Consume: %s", err)
		}
		handler.ready = make(chan struct{})

		select {
		case <-ctx.Done():
			break loop
		default:
		}
	}
	log.Printf("Closing Kafka source connector")
	close(ks.out)
	if err := ks.consumer.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// Via streams data to a specified operator and returns it.
func (ks *KafkaSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ks, operator)
	return operator
}

// Out returns the output channel of the KafkaSource connector.
func (ks *KafkaSource) Out() <-chan any {
	return ks.out
}

// GroupHandler implements the [sarama.ConsumerGroupHandler] interface.
// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for the consumer group session life-cycle and
// allows for triggering logic before or after the consume loop(s).
type GroupHandler struct {
	ready chan struct{}
	out   chan any
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (handler *GroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// mark the consumer as ready
	close(handler.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (handler *GroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (handler *GroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s",
					string(message.Value), message.Timestamp, message.Topic)
				session.MarkMessage(message, "") // mark the message as consumed
				handler.out <- message
			}

		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

// KafkaSink represents an Apache Kafka sink connector.
type KafkaSink struct {
	producer sarama.SyncProducer
	topic    string
	in       chan any
}

var _ streams.Sink = (*KafkaSink)(nil)

// NewKafkaSink returns a new KafkaSink connector.
func NewKafkaSink(addrs []string, config *sarama.Config, topic string) (*KafkaSink, error) {
	producer, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}

	sink := &KafkaSink{
		producer: producer,
		topic:    topic,
		in:       make(chan any),
	}
	go sink.init()

	return sink, nil
}

func (ks *KafkaSink) init() {
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
			log.Printf("Unsupported message type: %T", message)
		}

		if err != nil {
			log.Printf("Error processing Kafka message: %s", err)
		}
	}
	log.Printf("Closing Kafka sink connector")
	if err := ks.producer.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// In returns the input channel of the KafkaSink connector.
func (ks *KafkaSink) In() chan<- any {
	return ks.in
}
