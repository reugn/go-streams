package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/reugn/go-streams/flow"
	ext "github.com/reugn/go-streams/redis"
)

// https://redis.io/topics/pubsub
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

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
	log.Printf("Got: %s", msg)
	return strings.ToUpper(msg.Payload)
}
