package main

import (
	"context"
	"log"
	"strings"
	"time"

	ext "github.com/reugn/go-streams/redis"

	"github.com/redis/go-redis/v9"
	"github.com/reugn/go-streams/flow"
)

// docker exec -it pubsub bash
// https://redis.io/topics/pubsub
func main() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	timer := time.NewTimer(time.Minute)
	go func() {
		<-timer.C
		cancelFunc()
	}()

	config := &redis.Options{
		Addr:     "localhost:6379", // use default Addr
		Password: "",               // no password set
		DB:       0,                // use default DB
	}

	redisClient := redis.NewClient(config)

	source, err := ext.NewPubSubSource(ctx, redisClient, "test")
	if err != nil {
		log.Fatal(err)
	}

	toUpperMapFlow := flow.NewMap(toUpper, 1)
	sink := ext.NewPubSubSink(ctx, redisClient, "test2")

	source.
		Via(toUpperMapFlow).
		To(sink)
}

var toUpper = func(msg *redis.Message) string {
	return strings.ToUpper(msg.Payload)
}
