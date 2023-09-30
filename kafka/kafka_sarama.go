package kafka

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// KafkaSource represents an Apache Kafka source connector.
type KafkaSource struct {
	consumer  sarama.ConsumerGroup
	handler   sarama.ConsumerGroupHandler
	topics    []string
	out       chan interface{}
	ctx       context.Context
	cancelCtx context.CancelFunc
	wg        *sync.WaitGroup
}

// NewKafkaSource returns a new KafkaSource instance.
func NewKafkaSource(ctx context.Context, addrs []string, groupID string,
	config *sarama.Config, topics ...string) (*KafkaSource, error) {
	consumerGroup, err := sarama.NewConsumerGroup(addrs, groupID, config)
	if err != nil {
		return nil, err
	}

	out := make(chan interface{})
	cctx, cancel := context.WithCancel(ctx)

	sink := &KafkaSource{
		consumer:  consumerGroup,
		handler:   &GroupHandler{make(chan struct{}), out},
		topics:    topics,
		out:       out,
		ctx:       cctx,
		cancelCtx: cancel,
		wg:        &sync.WaitGroup{},
	}

	go sink.init()
	return sink, nil
}

func (ks *KafkaSource) claimLoop() {
	ks.wg.Add(1)
	defer func() {
		ks.wg.Done()
		log.Printf("Exiting Kafka claimLoop")
	}()
	for {
		handler := ks.handler.(*GroupHandler)
		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		if err := ks.consumer.Consume(ks.ctx, ks.topics, handler); err != nil {
			log.Printf("Kafka consumer.Consume failed with: %v", err)
		}

		select {
		case <-ks.ctx.Done():
			return
		default:
		}

		handler.ready = make(chan struct{})
	}
}

// init starts the main loop
func (ks *KafkaSource) init() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go ks.claimLoop()

	select {
	case <-sigchan:
		ks.cancelCtx()
	case <-ks.ctx.Done():
	}

	log.Printf("Closing Kafka consumer")
	ks.wg.Wait()
	close(ks.out)
	ks.consumer.Close()
}

// Via streams data through the given flow
func (ks *KafkaSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ks, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (ks *KafkaSource) Out() <-chan interface{} {
	return ks.out
}

// GroupHandler represents a Sarama consumer group handler
type GroupHandler struct {
	ready chan struct{}
	out   chan interface{}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (handler *GroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(handler.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (handler *GroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (handler *GroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s",
					string(message.Value), message.Timestamp, message.Topic)
				session.MarkMessage(message, "")
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
	in       chan interface{}
}

// NewKafkaSink returns a new KafkaSink instance.
func NewKafkaSink(addrs []string, config *sarama.Config, topic string) (*KafkaSink, error) {
	producer, err := sarama.NewSyncProducer(addrs, config)
	if err != nil {
		return nil, err
	}

	sink := &KafkaSink{
		producer: producer,
		topic:    topic,
		in:       make(chan interface{}),
	}

	go sink.init()
	return sink, nil
}

// init starts the main loop
func (ks *KafkaSink) init() {
	for msg := range ks.in {
		var err error
		switch m := msg.(type) {
		case *sarama.ProducerMessage:
			_, _, err = ks.producer.SendMessage(m)

		case *sarama.ConsumerMessage:
			sMsg := &sarama.ProducerMessage{
				Topic: ks.topic,
				Key:   sarama.StringEncoder(m.Key),
				Value: sarama.StringEncoder(m.Value),
			}
			_, _, err = ks.producer.SendMessage(sMsg)

		case string:
			sMsg := &sarama.ProducerMessage{
				Topic: ks.topic,
				Value: sarama.StringEncoder(m),
			}
			_, _, err = ks.producer.SendMessage(sMsg)

		default:
			log.Printf("Unsupported message type %v", m)
		}

		if err != nil {
			log.Printf("Error processing Kafka message: %s", err)
		}
	}

	log.Printf("Closing Kafka producer")
	ks.producer.Close()
}

// In returns an input channel for receiving data
func (ks *KafkaSink) In() chan<- interface{} {
	return ks.in
}
