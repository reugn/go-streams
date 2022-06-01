package main

import (
	"context"
	"log"
	"strings"
	"time"

	ext "github.com/reugn/go-streams/redis"

	"github.com/go-redis/redis"
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

	source, err := ext.NewRedisSource(ctx, config, "test")
	if err != nil {
		log.Fatal(err)
	}
	flow1 := flow.NewMap(toUpper, 1)
	sink := ext.NewRedisSink(config, "test2")

	source.
		Via(flow1).
		To(sink)
}

var toUpper = func(in interface{}) interface{} {
	msg := in.(*redis.Message)
	return strings.ToUpper(msg.Payload)
}
