package gcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
	"google.golang.org/api/iterator"
)

// StorageSourceConfig represents the configuration options for the GCP Storage
// source connector.
type StorageSourceConfig struct {
	// The name of the storage bucket to read from.
	Bucket string
	// The path within the bucket to use. If empty, the root of the
	// bucket will be used.
	Prefix string
	// Delimiter can be used to restrict the results to only the objects in
	// the given "directory". Without the delimiter, the entire tree under
	// the prefix is returned.
	Delimiter string
}

// StorageSource represents the Google Cloud Storage source connector.
type StorageSource struct {
	client *storage.Client
	config *StorageSourceConfig
	out    chan any

	logger *slog.Logger
}

var _ streams.Source = (*StorageSource)(nil)

// NewStorageSource returns a new [StorageSource].
// The connector reads all objects within the configured path and transmits
// them as an [StorageObject] through the output channel.
func NewStorageSource(ctx context.Context, client *storage.Client,
	config *StorageSourceConfig, logger *slog.Logger) *StorageSource {

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "gcp.storage"),
		slog.String("type", "source")))

	storageSource := &StorageSource{
		client: client,
		config: config,
		out:    make(chan any),
		logger: logger,
	}

	// read objects and send them downstream
	go storageSource.readObjects(ctx)

	return storageSource
}

func (s *StorageSource) readObjects(ctx context.Context) {
	defer func() {
		s.logger.Info("Closing connector")
		close(s.out)
	}()

	bucketHandle := s.client.Bucket(s.config.Bucket)
	// check if the bucket exists
	if _, err := bucketHandle.Attrs(ctx); err != nil {
		s.logger.Error("Failed to get bucket attrs",
			slog.String("bucket", s.config.Bucket), slog.Any("error", err))
		return
	}

	// create objects iterator
	it := bucketHandle.Objects(ctx, &storage.Query{
		Prefix:    s.config.Prefix,
		Delimiter: s.config.Delimiter,
	})

	for { // iterate over the objects in the bucket
		objectAttrs, err := it.Next()
		if err != nil {
			if !errors.Is(err, iterator.Done) {
				s.logger.Error("Failed to read object from bucket",
					slog.String("bucket", s.config.Bucket), slog.Any("error", err))
			}
			// If the previous call to Next returned an error other than iterator.Done,
			// all subsequent calls will return the same error. To continue iteration,
			// a new `ObjectIterator` must be created.
			return
		}

		// create a new reader to read the contents of the object
		reader, err := bucketHandle.Object(objectAttrs.Name).NewReader(ctx)
		if err != nil {
			s.logger.Error("Failed to create reader from object",
				slog.String("object", objectAttrs.Name),
				slog.String("bucket", s.config.Bucket), slog.Any("error", err))
			continue
		}

		select {
		// send the object downstream
		case s.out <- &StorageObject{
			Key:  objectAttrs.Name,
			Data: reader,
		}:
		case <-ctx.Done():
			s.logger.Debug("Object reading terminated", slog.Any("error", ctx.Err()))
			return
		}
	}
}

// Via asynchronously streams data to the given Flow and returns it.
func (s *StorageSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(s, operator)
	return operator
}

// Out returns the output channel of the StorageSource connector.
func (s *StorageSource) Out() <-chan any {
	return s.out
}

// StorageObject contains details of the GCP Storage object.
type StorageObject struct {
	// Key is the object name including any subdirectories.
	// For example, "directory/file.json".
	Key string
	// Data is an [io.ReadCloser] representing the binary content of the object.
	Data io.ReadCloser
}

// StorageSinkConfig represents the configuration options for the GCP Storage
// sink connector.
type StorageSinkConfig struct {
	// The name of the GCP Storage bucket to write to.
	Bucket string
	// The number of concurrent workers to use when writing data to GCP Storage.
	// The default is 1.
	Parallelism int
}

// StorageSink represents the Google Cloud Storage sink connector.
type StorageSink struct {
	client *storage.Client
	config *StorageSinkConfig
	in     chan any

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*StorageSink)(nil)

// NewStorageSink returns a new [StorageSink].
// Incoming elements are expected to be of the [StorageObject] type. These will
// be uploaded to the configured bucket using their key field as the path.
func NewStorageSink(ctx context.Context, client *storage.Client,
	config *StorageSinkConfig, logger *slog.Logger) *StorageSink {

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "gcp.storage"),
		slog.String("type", "sink")))

	if config.Parallelism < 1 {
		config.Parallelism = 1
	}

	storageSink := &StorageSink{
		client: client,
		config: config,
		in:     make(chan any, config.Parallelism),
		done:   make(chan struct{}),
		logger: logger,
	}

	// start writing incoming data
	go storageSink.writeObjects(ctx)

	return storageSink
}

// writeObjects writes incoming stream data elements to GCP Storage using the
// configured parallelism.
func (s *StorageSink) writeObjects(ctx context.Context) {
	defer close(s.done) // signal data processing completion

	bucketHandle := s.client.Bucket(s.config.Bucket)
	var wg sync.WaitGroup
	for i := 0; i < s.config.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range s.in {
				var err error
				switch object := data.(type) {
				case StorageObject:
					err = s.writeObject(ctx, bucketHandle, &object)
				case *StorageObject:
					err = s.writeObject(ctx, bucketHandle, object)
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

// writeObject writes a single object to GCP Storage.
func (s *StorageSink) writeObject(ctx context.Context, bucketHandle *storage.BucketHandle,
	object *StorageObject) error {
	defer func() {
		if err := object.Data.Close(); err != nil {
			s.logger.Warn("Failed to close object",
				slog.String("key", object.Key),
				slog.Any("error", err))
		}
	}()

	// writes will be retried on transient errors from the server
	writer := bucketHandle.Object(object.Key).NewWriter(ctx)
	if _, err := io.Copy(writer, object.Data); err != nil {
		return fmt.Errorf("failed to write object %s: %w", object.Key, err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer %s: %w", object.Key, err)
	}

	s.logger.Debug("Object successfully written", slog.String("key", object.Key))

	return nil
}

// In returns the input channel of the StorageSink connector.
func (s *StorageSink) In() chan<- any {
	return s.in
}

// AwaitCompletion blocks until the StorageSink connector has completed
// processing all the received data.
func (s *StorageSink) AwaitCompletion() {
	<-s.done
}
