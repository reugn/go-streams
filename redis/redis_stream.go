package redis

import (
	"context"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// StreamSource represents a Redis stream source connector.
//
// A Redis stream is a data structure that acts like an append-only log but
// also implements several operations to overcome some of the limits of a typical
// append-only log. These include random access in O(1) time and complex
// consumption strategies, such as consumer groups.
type StreamSource struct {
	redisClient     *redis.Client
	readGroupArgs   *redis.XReadGroupArgs
	groupCreateArgs *XGroupCreateArgs
	out             chan any
}

var _ streams.Source = (*StreamSource)(nil)

// XGroupCreateArgs represents the arguments for creating a consumer group.
//
// Use the special StartID "$" to fetch only the new elements arriving in the stream.
// If instead you want the consumer to fetch the whole stream history,
// use zero ("0") as the starting ID for the consumer group.
type XGroupCreateArgs struct {
	Stream   string
	Group    string
	StartID  string
	MkStream bool // set to true to create an empty stream automatically
}

// NewStreamSource returns a new StreamSource instance.
// Pass in nil for the groupCreateArgs parameter if the consumer group already exists.
func NewStreamSource(ctx context.Context, redisClient *redis.Client,
	readGroupArgs *redis.XReadGroupArgs, groupCreateArgs *XGroupCreateArgs,
) (*StreamSource, error) {
	if groupCreateArgs != nil {
		// Create a new consumer group uniquely identified by <group> for the stream
		// stored at <stream>.
		// By default, the XGROUP CREATE command expects that the target stream exists,
		// and returns an error when it doesn't.
		var err error
		if groupCreateArgs.MkStream {
			err = redisClient.XGroupCreateMkStream(
				ctx,
				groupCreateArgs.Stream,
				groupCreateArgs.Group,
				groupCreateArgs.StartID).Err()
		} else {
			err = redisClient.XGroupCreate(
				ctx,
				groupCreateArgs.Stream,
				groupCreateArgs.Group,
				groupCreateArgs.StartID).Err()
		}
		if err != nil {
			return nil, err
		}
	}

	source := &StreamSource{
		redisClient:     redisClient,
		readGroupArgs:   readGroupArgs,
		groupCreateArgs: groupCreateArgs,
		out:             make(chan any),
	}
	go source.init(ctx)

	return source, nil
}

func (rs *StreamSource) init(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			// The XREADGROUP command is a special version of the XREAD command with
			// support for consumer groups.
			entries, err := rs.redisClient.XReadGroup(ctx, rs.readGroupArgs).Result()
			if err != nil {
				log.Printf("Error in XReadGroup: %s", err)
				if strings.HasPrefix(err.Error(), "NOGROUP") {
					break loop
				}
			}
			// route incoming messages downstream
			for _, stream := range entries {
				for _, msg := range stream.Messages {
					rs.out <- &msg
				}
			}
		}
	}
	log.Printf("Closing Redis StreamSource connector")
	close(rs.out)
	if err := rs.redisClient.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// Via streams data to a specified operator and returns it.
func (rs *StreamSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(rs, operator)
	return operator
}

// Out returns the output channel of the StreamSource connector.
func (rs *StreamSource) Out() <-chan any {
	return rs.out
}

// StreamSink represents a Redis stream sink connector.
type StreamSink struct {
	redisClient *redis.Client
	stream      string
	in          chan any
}

var _ streams.Sink = (*StreamSink)(nil)

// NewStreamSink returns a new StreamSink instance.
//
// The incoming messages will be streamed to the given target stream using the
// provided redis.Client.
func NewStreamSink(ctx context.Context, redisClient *redis.Client,
	stream string) *StreamSink {
	sink := &StreamSink{
		redisClient: redisClient,
		stream:      stream,
		in:          make(chan any),
	}
	go sink.init(ctx)

	return sink
}

func (rs *StreamSink) init(ctx context.Context) {
	for msg := range rs.in {
		switch message := msg.(type) {
		case *redis.XMessage:
			rs.xAdd(ctx, &redis.XAddArgs{
				Stream: rs.stream, // use the target stream name
				Values: message.Values,
			})
		case map[string]any:
			rs.xAdd(ctx, &redis.XAddArgs{
				Stream: rs.stream,
				Values: message,
			})
		default:
			log.Printf("Unsupported message type: %T", message)
		}
	}
	log.Printf("Closing Redis StreamSink connector")
	if err := rs.redisClient.Close(); err != nil {
		log.Printf("Error in Close: %s", err)
	}
}

// xAdd appends the message to the target stream.
func (rs *StreamSink) xAdd(ctx context.Context, args *redis.XAddArgs) {
	// Streams are an append-only data structure. The fundamental write
	// command, called XADD, appends a new entry to the specified stream.
	if err := rs.redisClient.XAdd(ctx, args).Err(); err != nil {
		log.Printf("Error in XAdd: %s", err)
	}
}

// In returns the input channel of the StreamSink connector.
func (rs *StreamSink) In() chan<- any {
	return rs.in
}
