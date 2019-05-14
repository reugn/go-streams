package ext

import (
	"log"

	"github.com/go-redis/redis"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// Redis Pub/Sub Source implementation
type RedisSource struct {
	redisdb *redis.Client
	channel string
	out     chan interface{}
}

func NewRedisSource(config *redis.Options, channel string) (*RedisSource, error) {
	redisdb := redis.NewClient(config)
	pubsub := redisdb.Subscribe(channel)

	// Wait for confirmation that subscription is created before publishing anything
	_, err := pubsub.Receive()
	if err != nil {
		return nil, err
	}
	source := &RedisSource{
		redisdb,
		channel,
		make(chan interface{}),
	}
	go source.init(pubsub.Channel())
	return source, nil
}

func (rs *RedisSource) init(ch <-chan *redis.Message) {
	for msg := range ch {
		rs.out <- msg
	}
}

func (rs *RedisSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(rs, _flow)
	return _flow
}

func (rs *RedisSource) Out() <-chan interface{} {
	return rs.out
}

// Redis Pub/Sub Sink implementation
type RedisSink struct {
	redisdb *redis.Client
	channel string
	in      chan interface{}
}

func NewRedisSink(config *redis.Options, channel string) *RedisSink {
	sink := &RedisSink{
		redis.NewClient(config),
		channel,
		make(chan interface{}),
	}
	go sink.init()
	return sink
}

//start main loop
func (rs *RedisSink) init() {
	for msg := range rs.in {
		switch m := msg.(type) {
		case string:
			log.Println("Redis message:", m)
			err := rs.redisdb.Publish(rs.channel, m).Err()
			if err != nil {
				panic(err)
			}
		}
	}
}

func (rs *RedisSink) In() chan<- interface{} {
	return rs.in
}
