package aws

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

const s3DefaultChunkSize = 5 * 1024 * 1024 // 5 MB

// S3SourceConfig represents the configuration options for the S3 source
// connector.
type S3SourceConfig struct {
	// The name of the S3 bucket to read from.
	Bucket string
	// The path within the bucket to use. If empty, the root of the
	// bucket will be used.
	Path string
	// The number of concurrent workers to use when reading data from S3.
	// The default is 1.
	Parallelism int
	// The size of chunks in bytes to use when reading data from S3.
	// The default is 5 MB.
	ChunkSize int
}

// S3Source represents the AWS S3 source connector.
type S3Source struct {
	client   *s3.Client
	config   *S3SourceConfig
	objectCh chan string
	out      chan any

	logger *slog.Logger
}

var _ streams.Source = (*S3Source)(nil)

// NewS3Source returns a new [S3Source].
// The connector reads all objects within the configured path and transmits
// them as an [S3Object] through the output channel.
func NewS3Source(ctx context.Context, client *s3.Client,
	config *S3SourceConfig, logger *slog.Logger) *S3Source {

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "aws.s3"),
		slog.String("type", "source")))

	if config.Parallelism < 1 {
		config.Parallelism = 1
	}
	if config.ChunkSize < 1 {
		config.ChunkSize = s3DefaultChunkSize
	}

	s3Source := &S3Source{
		client:   client,
		config:   config,
		objectCh: make(chan string, config.Parallelism),
		out:      make(chan any),
		logger:   logger,
	}

	// list objects in the configured path
	go s3Source.listObjects(ctx)

	// read the objects and send data downstream
	go s3Source.getObjects(ctx)

	return s3Source
}

// listObjects reads the list of objects in the configured path and streams
// the keys to the objectCh channel.
func (s *S3Source) listObjects(ctx context.Context) {
	var continuationToken *string
	for {
		listResponse, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &s.config.Bucket,
			Prefix:            &s.config.Path,
			ContinuationToken: continuationToken,
		})

		if err != nil {
			s.logger.Error("Failed to list objects", slog.Any("error", err),
				slog.Any("continuationToken", continuationToken))
			break
		}

		for _, object := range listResponse.Contents {
			s.objectCh <- *object.Key
		}

		continuationToken = listResponse.NextContinuationToken
		if continuationToken == nil {
			break
		}
	}
	// close the objects channel
	close(s.objectCh)
}

// getObjects reads the objects data and sends it downstream.
func (s *S3Source) getObjects(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < s.config.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
		loop:
			for {
				select {
				case key, ok := <-s.objectCh:
					if !ok {
						break loop
					}
					objectOutput, err := s.client.GetObject(ctx, &s3.GetObjectInput{
						Bucket: &s.config.Bucket,
						Key:    &key,
					})
					if err != nil {
						s.logger.Error("Failed to get object", slog.Any("error", err),
							slog.String("key", key))
					}

					data := make([]byte, s.config.ChunkSize)
					n, err := bufio.NewReaderSize(objectOutput.Body, s.config.ChunkSize).
						Read(data)
					if err != nil {
						s.logger.Error("Failed to read object", slog.Any("error", err),
							slog.String("key", key))
						continue
					}

					s.logger.Debug("Successfully read object", slog.String("key", key),
						slog.Int("size", n))

					// send the read data downstream as an S3Object
					s.out <- &S3Object{
						Key:  key,
						Data: bytes.NewReader(data),
					}
				case <-ctx.Done():
					s.logger.Debug("Object reading terminated", slog.Any("error", ctx.Err()))
					break loop
				}
			}
		}()
	}

	// wait for all object readers to exit
	wg.Wait()
	s.logger.Info("Closing connector")
	// close the output channel
	close(s.out)
}

// Via asynchronously streams data to the given Flow and returns it.
func (s *S3Source) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(s, operator)
	return operator
}

// Out returns the output channel of the S3Source connector.
func (s *S3Source) Out() <-chan any {
	return s.out
}

// S3Object contains details of the S3 object.
type S3Object struct {
	// Key is the object name including any subdirectories.
	// For example, "directory/file.json".
	Key string
	// Data is an [io.Reader] representing the binary content of the object.
	// This can be a file, a buffer, or any other type that implements the
	// io.Reader interface.
	Data io.Reader
}

// S3SinkConfig represents the configuration options for the S3 sink
// connector.
type S3SinkConfig struct {
	// The name of the S3 bucket to write to.
	Bucket string
	// The number of concurrent workers to use when writing data to S3.
	// The default is 1.
	Parallelism int
}

// S3Sink represents the AWS S3 sink connector.
type S3Sink struct {
	client *s3.Client
	config *S3SinkConfig
	in     chan any

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*S3Sink)(nil)

// NewS3Sink returns a new [S3Sink].
// Incoming elements are expected to be of the [S3Object] type. These will
// be uploaded to the configured bucket using their key field as the path.
func NewS3Sink(ctx context.Context, client *s3.Client,
	config *S3SinkConfig, logger *slog.Logger) *S3Sink {

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "aws.s3"),
		slog.String("type", "sink")))

	if config.Parallelism < 1 {
		config.Parallelism = 1
	}

	s3Sink := &S3Sink{
		client: client,
		config: config,
		in:     make(chan any, config.Parallelism),
		done:   make(chan struct{}),
		logger: logger,
	}

	// start writing incoming data
	go s3Sink.writeObjects(ctx)

	return s3Sink
}

// writeObjects writes incoming stream data elements to S3 using the
// configured parallelism.
func (s *S3Sink) writeObjects(ctx context.Context) {
	defer close(s.done) // signal data processing completion

	var wg sync.WaitGroup
	for i := 0; i < s.config.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range s.in {
				var err error
				switch object := data.(type) {
				case S3Object:
					err = s.writeObject(ctx, &object)
				case *S3Object:
					err = s.writeObject(ctx, object)
				default:
					s.logger.Error("Unsupported data type",
						slog.String("type", fmt.Sprintf("%T", object)))
				}

				if err != nil {
					s.logger.Error("Error writing object",
						slog.Any("error", err))
				}
			}
		}()
	}

	// wait for all writers to exit
	wg.Wait()
	s.logger.Info("All object writers exited")
}

// writeObject writes a single object to S3.
func (s *S3Sink) writeObject(ctx context.Context, putObject *S3Object) error {
	putObjectOutput, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.config.Bucket,
		Key:    &putObject.Key,
		Body:   putObject.Data,
	})
	if err != nil {
		return fmt.Errorf("failed to put object %s: %w", putObject.Key, err)
	}

	s.logger.Debug("Object successfully stored", slog.String("key", putObject.Key),
		slog.Any("etag", putObjectOutput.ETag))

	return nil
}

// In returns the input channel of the S3Sink connector.
func (s *S3Sink) In() chan<- any {
	return s.in
}

// AwaitCompletion blocks until the S3Sink connector has processed all the received data.
func (s *S3Sink) AwaitCompletion() {
	<-s.done
}
