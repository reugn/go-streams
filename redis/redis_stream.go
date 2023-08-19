package redis

import (
	"context"
	"log"

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
	ctx             context.Context
	redisClient     *redis.Client
	readGroupArgs   *redis.XReadGroupArgs
	groupCreateArgs *XGroupCreateArgs
	out             chan interface{}
}

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
	readGroupArgs *redis.XReadGroupArgs, groupCreateArgs *XGroupCreateArgs) (*StreamSource, error) {
	if groupCreateArgs != nil {
		// Create a new consumer group uniquely identified by <group> for the stream stored at <stream>.
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
		ctx:             ctx,
		redisClient:     redisClient,
		readGroupArgs:   readGroupArgs,
		groupCreateArgs: groupCreateArgs,
		out:             make(chan interface{}),
	}

	go source.init()
	return source, nil
}

// init starts the main loop
func (rs *StreamSource) init() {
loop:
	for {
		select {
		case <-rs.ctx.Done():
			break loop

		default:
			// The XREADGROUP command is a special version of the XREAD command with
			// support for consumer groups.
			entries, err := rs.redisClient.XReadGroup(rs.ctx, rs.readGroupArgs).Result()
			if err != nil {
				log.Printf("Error in redisClient.XReadGroup: %s", err)
			}

			for _, e := range entries {
				for _, msg := range e.Messages {
					rs.out <- &msg
				}
			}
		}
	}

	log.Printf("Closing Redis stream consumer")
	close(rs.out)
	rs.redisClient.Close()
}

// Via streams data through the given flow
func (rs *StreamSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(rs, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (rs *StreamSource) Out() <-chan interface{} {
	return rs.out
}

// StreamSink represents a Redis stream sink connector.
type StreamSink struct {
	ctx         context.Context
	redisClient *redis.Client
	stream      string
	in          chan interface{}
}

// NewStreamSink returns a new StreamSink instance.
//
// The incoming messages will be streamed to the given target stream using the
// provided redis.Client.
func NewStreamSink(ctx context.Context, redisClient *redis.Client, stream string) *StreamSink {
	sink := &StreamSink{
		ctx:         ctx,
		redisClient: redisClient,
		stream:      stream,
		in:          make(chan interface{}),
	}

	go sink.init()
	return sink
}

// init starts the main loop
func (rs *StreamSink) init() {
	for msg := range rs.in {
		switch m := msg.(type) {
		case *redis.XMessage:
			rs.xAdd(&redis.XAddArgs{
				Stream: rs.stream, // use the target stream name
				Values: m.Values,
			})
		case map[string]interface{}:
			rs.xAdd(&redis.XAddArgs{
				Stream: rs.stream,
				Values: m,
			})
		default:
			log.Printf("Unsupported message type %v", m)
		}
	}

	log.Printf("Closing Redis stream producer")
	rs.redisClient.Close()
}

// xAdd appends the message to the target stream
func (rs *StreamSink) xAdd(args *redis.XAddArgs) {
	// Streams are an append-only data structure. The fundamental write
	// command, called XADD, appends a new entry to the specified stream.
	err := rs.redisClient.XAdd(rs.ctx, args).Err()
	if err != nil {
		log.Printf("Error in redisClient.XAdd: %s", err)
	}
}

// In returns an input channel for receiving data
func (rs *StreamSink) In() chan<- interface{} {
	return rs.in
}
