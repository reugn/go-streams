package ext

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// KafkaSource implementation
// supports rebalance handling for given group.id
// auto.commit is enabled by default
type KafkaSource struct {
	consumer   *kafka.Consumer
	topics     []string
	in         chan interface{}
	once       sync.Once
	commitFlow *CommitOffset
}

// Commit returns new commit Flow
func (ks *KafkaSource) Commit() *CommitOffset {
	ks.once.Do(func() {
		ks.commitFlow = NewCommitOffset(ks.consumer)
	})
	return ks.commitFlow
}

// CommitOffset is a Kafka manual offsets commit flow
type CommitOffset struct {
	consumer *kafka.Consumer
	in       chan interface{}
}

// NewCommitOffset returns new CommitOffset instance
func NewCommitOffset(consumer *kafka.Consumer) *CommitOffset {
	return &CommitOffset{
		consumer,
		make(chan interface{}),
	}
}

// Via streams data through given flow
func (co *CommitOffset) Via(flow streams.Flow) streams.Flow {
	go co.loop(flow)
	return flow
}

// To streams data to given sink
func (co *CommitOffset) To(sink streams.Sink) {
	co.loop(sink)
}

func (co *CommitOffset) loop(inlet streams.Inlet) {
	for ev := range co.Out() {
		switch e := ev.(type) {
		case *kafka.Message:
			co.consumer.CommitMessage(e)
			inlet.In() <- e
		default:
			panic("CommitOffset invalid msg type")
		}
	}
	close(inlet.In())
}

// Out returns channel for sending data
func (co *CommitOffset) Out() <-chan interface{} {
	return co.in
}

// In returns channel for receiving data
func (co *CommitOffset) In() chan<- interface{} {
	return co.in
}

// NewKafkaSource returns new KafkaSource instance
func NewKafkaSource(config *kafka.ConfigMap, topics ...string) *KafkaSource {
	consumer, err := kafka.NewConsumer(config)
	streams.Check(err)
	source := &KafkaSource{
		consumer,
		topics,
		make(chan interface{}),
		sync.Once{},
		nil,
	}
	go source.init()
	return source
}

// start main loop
func (ks *KafkaSource) init() {
	ks.consumer.SubscribeTopics(ks.topics, rebalanceCallback)
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := ks.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				log.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					log.Printf("%% Headers: %v\n", e.Headers)
				}
				ks.in <- e
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}
	log.Printf("Closing consumer\n")
	close(ks.in)
	ks.consumer.Close()
}

// Via streams data through given flow
func (ks *KafkaSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(ks, _flow)
	return _flow
}

// Out returns channel for sending data
func (ks *KafkaSource) Out() <-chan interface{} {
	return ks.in
}

// handle rebalance events
var rebalanceCallback = func(c *kafka.Consumer, e kafka.Event) error {
	switch evt := e.(type) {
	case *kafka.AssignedPartitions:
		c.Assign(evt.Partitions)
	case *kafka.RevokedPartitions:
		assigned, err := c.Assignment()
		if err != nil {
			return err
		}
		partitions := clearUnassigned(assigned, evt.Partitions)
		c.Assign(partitions)
	}
	return nil
}

// clear unassigned partitions from current assignment
func clearUnassigned(a []kafka.TopicPartition, u []kafka.TopicPartition) []kafka.TopicPartition {
	var rt []kafka.TopicPartition
	for _, p := range a {
		if !contains(u, p) {
			rt = append(rt, p)
		}
	}
	return rt
}

func contains(s []kafka.TopicPartition, p kafka.TopicPartition) bool {
	for _, sp := range s {
		if sp == p {
			return true
		}
	}
	return false
}

// KafkaSink implementation
// Produces messages using Round Robin partitioning strategy on empty key
type KafkaSink struct {
	producer        *kafka.Producer
	topic           string
	in              chan interface{}
	partition       int32
	topicPartitions int32
}

// NewKafkaSink returns new KafkaSink instance
func NewKafkaSink(config *kafka.ConfigMap, topic string) *KafkaSink {
	producer, err := kafka.NewProducer(config)
	streams.Check(err)
	sink := &KafkaSink{
		producer,
		topic,
		make(chan interface{}),
		0,
		topicPartitionsNumber(producer, topic),
	}
	go sink.init()
	return sink
}

// start main loop
func (ks *KafkaSink) init() {
	for msg := range ks.in {
		switch m := msg.(type) {
		case *kafka.Message:
			ks.produce(m.Key, m.Value, m.Headers)
		case string:
			ks.produce(nil, []byte(m), []kafka.Header{})
		}
	}
	log.Printf("Closing producer\n")
	ks.producer.Close()
}

// produce message
func (ks *KafkaSink) produce(key []byte, value []byte, headers []kafka.Header) error {
	var partition int32
	if key == nil {
		partition = ks.nextPartition()
	} else {
		partition = ks.keyPartition(key)
	}
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &ks.topic, Partition: partition},
		Value:          value,
		Key:            key,
		Headers:        headers,
	}
	log.Printf("Producing message: %s, to topic: %s\n", msg.Value, msg.TopicPartition.String())
	return ks.producer.Produce(&msg, nil)
}

// get topic partition number
func topicPartitionsNumber(producer *kafka.Producer, topic string) int32 {
	metadata, err := producer.GetMetadata(&topic, false, 5000)
	streams.Check(err)
	return int32(len(metadata.Topics[topic].Partitions))
}

// Round Robin partitioning strategy
// thread-safe, used from main loop goroutine only
func (ks *KafkaSink) nextPartition() int32 {
	ks.partition++
	partition := (ks.partition & 0x7fffffff) % ks.topicPartitions
	log.Printf("Partition: %d from %d\n", partition, ks.topicPartitions)
	return partition
}

// Calculate message partition by key
func (ks *KafkaSink) keyPartition(key []byte) int32 {
	hashCode := streams.HashCode(key)
	return (int32(hashCode) & 0x7fffffff) % ks.topicPartitions
}

// In returns channel for receiving data
func (ks *KafkaSink) In() chan<- interface{} {
	return ks.in
}
