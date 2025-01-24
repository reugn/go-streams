package redis

import (
	"context"
	"fmt"
	"log/slog"
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

	logger *slog.Logger
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

// NewStreamSource returns a new [StreamSource] connector.
// Pass in nil for the groupCreateArgs parameter if the consumer group already exists.
func NewStreamSource(ctx context.Context, redisClient *redis.Client,
	readGroupArgs *redis.XReadGroupArgs, groupCreateArgs *XGroupCreateArgs,
	logger *slog.Logger,
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
			return nil, fmt.Errorf("failed to create consumer group: %w", err)
		}
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "redis.stream"),
		slog.String("type", "source")))

	source := &StreamSource{
		redisClient:     redisClient,
		readGroupArgs:   readGroupArgs,
		groupCreateArgs: groupCreateArgs,
		out:             make(chan any),
		logger:          logger,
	}

	// asynchronously consume data and send it downstream
	go source.process(ctx)

	return source, nil
}

func (rs *StreamSource) process(ctx context.Context) {
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
				rs.logger.Error("Error in client.XReadGroup", slog.Any("error", err))
				if strings.HasPrefix(err.Error(), "NOGROUP") {
					break loop
				}
			}
			// route incoming messages downstream
			for _, stream := range entries {
				for _, message := range stream.Messages {
					rs.out <- &message
				}
			}
		}
	}

	rs.logger.Info("Closing connector")
	close(rs.out)
	if err := rs.redisClient.Close(); err != nil {
		rs.logger.Warn("Error in client.Close", slog.Any("error", err))
	}
}

// Via asynchronously streams data to the given Flow and returns it.
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

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*StreamSink)(nil)

// NewStreamSink returns a new [StreamSink] connector.
//
// The incoming messages will be streamed to the given target stream using the
// provided redis.Client.
func NewStreamSink(ctx context.Context, redisClient *redis.Client,
	stream string, logger *slog.Logger) *StreamSink {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "redis.stream"),
		slog.String("type", "sink")))

	sink := &StreamSink{
		redisClient: redisClient,
		stream:      stream,
		in:          make(chan any),
		done:        make(chan struct{}),
		logger:      logger,
	}

	// begin processing upstream data
	go sink.process(ctx)

	return sink
}

func (rs *StreamSink) process(ctx context.Context) {
	defer close(rs.done) // signal data processing completion

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
			rs.logger.Error("Unsupported message type",
				slog.String("type", fmt.Sprintf("%T", message)))
		}
	}

	rs.logger.Info("Closing connector")
	if err := rs.redisClient.Close(); err != nil {
		rs.logger.Warn("Error in client.Close", slog.Any("error", err))
	}
}

// xAdd appends the message to the target stream.
func (rs *StreamSink) xAdd(ctx context.Context, args *redis.XAddArgs) {
	// Streams are an append-only data structure. The fundamental write
	// command, called XADD, appends a new entry to the specified stream.
	if err := rs.redisClient.XAdd(ctx, args).Err(); err != nil {
		rs.logger.Error("Error in client.XAdd", slog.Any("error", err))
	}
}

// In returns the input channel of the StreamSink connector.
func (rs *StreamSink) In() chan<- any {
	return rs.in
}

// AwaitCompletion blocks until the StreamSink connector has completed
// processing all the received data.
func (rs *StreamSink) AwaitCompletion() {
	<-rs.done
}
