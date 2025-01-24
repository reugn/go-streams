package azure

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// BlobStorageSourceConfig represents the configuration options for the Azure
// Blob storage source connector.
type BlobStorageSourceConfig struct {
	// The name of the Azure Blob storage container to read from.
	Container string
	// The path within the container to use. If empty, the root of the container will be used.
	Prefix string
	// Indicates whether to ignore blob prefixes (virtual directories) in blob segments.
	Flat bool
}

// BlobStorageSource represents the Azure Blob storage source connector.
type BlobStorageSource struct {
	client          *azblob.Client
	containerClient *container.Client
	config          *BlobStorageSourceConfig
	out             chan any

	logger *slog.Logger
}

var _ streams.Source = (*BlobStorageSource)(nil)

// NewBlobStorageSource returns a new [BlobStorageSource].
// The connector reads all objects within the configured path and transmits
// them as a [BlobStorageObject] through the output channel.
func NewBlobStorageSource(ctx context.Context, client *azblob.Client,
	config *BlobStorageSourceConfig, logger *slog.Logger) *BlobStorageSource {

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "azure.blob"),
		slog.String("type", "source")))

	blobSource := &BlobStorageSource{
		client:          client,
		containerClient: client.ServiceClient().NewContainerClient(config.Container),
		config:          config,
		out:             make(chan any),
		logger:          logger,
	}

	// read objects and send them downstream
	go blobSource.listBlobs(ctx)

	return blobSource
}

func (s *BlobStorageSource) listBlobs(ctx context.Context) {
	s.listBlobsHierarchy(ctx, &s.config.Prefix, nil)

	s.logger.Info("Blob iteration completed")
	close(s.out)
}

func (s *BlobStorageSource) listBlobsHierarchy(ctx context.Context, prefix, marker *string) {
	pager := s.containerClient.NewListBlobsHierarchyPager("/",
		&container.ListBlobsHierarchyOptions{
			Prefix: prefix,
			Marker: marker,
		})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			s.logger.Error("Error reading next page", slog.Any("error", err))
			break
		}

		// set the continuation token
		marker = resp.Marker

		if !s.config.Flat && resp.Segment.BlobPrefixes != nil {
			for _, prefix := range resp.Segment.BlobPrefixes {
				s.logger.Debug("Virtual directory", slog.String("prefix", *prefix.Name))

				// recursively list blobs in the prefix
				s.listBlobsHierarchy(ctx, prefix.Name, nil)
			}
		}

		// list blobs in the current page
		for _, blob := range resp.Segment.BlobItems {
			resp, err := s.client.DownloadStream(ctx, s.config.Container, *blob.Name, nil)
			if err != nil {
				s.logger.Error("Error reading blob", slog.Any("error", err))
				continue
			}

			select {
			// send the blob downstream
			case s.out <- &BlobStorageObject{
				Key:  *blob.Name,
				Data: resp.Body,
			}:
			case <-ctx.Done():
				s.logger.Info("Blob reading terminated", slog.Any("error", ctx.Err()))
				return
			}
		}
	}

	// retrieve the remainder if the continuation token is available
	if marker != nil && *marker != "" {
		s.logger.Info("Continuation token is available", slog.String("marker", *marker))
		s.listBlobsHierarchy(ctx, prefix, marker)
	}
}

// Via asynchronously streams data to the given Flow and returns it.
func (s *BlobStorageSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(s, operator)
	return operator
}

// Out returns the output channel of the BlobStorageSource connector.
func (s *BlobStorageSource) Out() <-chan any {
	return s.out
}

// BlobStorageObject contains details of the Azure Blob storage object.
type BlobStorageObject struct {
	// Key is the object name including any subdirectories.
	// For example, "directory/file.json".
	Key string
	// Data is an [io.ReadCloser] representing the binary content of the blob object.
	Data io.ReadCloser
}

// BlobStorageSinkConfig represents the configuration options for the Azure Blob
// storage sink connector.
type BlobStorageSinkConfig struct {
	// The name of the Azure Blob storage container to write to.
	Container string
	// The number of concurrent workers to use when writing data to Azure Blob storage.
	// The default is 1.
	Parallelism int
	// UploadOptions specifies set of configurations for the blob upload operation.
	UploadOptions *blockblob.UploadStreamOptions
}

// BlobStorageSink represents the Azure Blob storage sink connector.
type BlobStorageSink struct {
	client *azblob.Client
	config *BlobStorageSinkConfig
	in     chan any

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*BlobStorageSink)(nil)

// NewBlobStorageSink returns a new [BlobStorageSink].
// Incoming elements are expected to be of the [BlobStorageObject] type. These will
// be uploaded to the configured container using their key field as the path.
func NewBlobStorageSink(ctx context.Context, client *azblob.Client,
	config *BlobStorageSinkConfig, logger *slog.Logger) *BlobStorageSink {

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "azure.blob"),
		slog.String("type", "sink")))

	if config.Parallelism < 1 {
		config.Parallelism = 1
	}

	blobSink := &BlobStorageSink{
		client: client,
		config: config,
		in:     make(chan any, config.Parallelism),
		done:   make(chan struct{}),
		logger: logger,
	}

	// start writing incoming data
	go blobSink.uploadBlobs(ctx)

	return blobSink
}

// uploadBlobs writes incoming stream data elements to Azure Blob storage
// using the configured parallelism.
func (s *BlobStorageSink) uploadBlobs(ctx context.Context) {
	defer close(s.done) // signal data processing completion

	var wg sync.WaitGroup
	for i := 0; i < s.config.Parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range s.in {
				var err error
				switch object := data.(type) {
				case BlobStorageObject:
					err = s.uploadBlob(ctx, &object)
				case *BlobStorageObject:
					err = s.uploadBlob(ctx, object)
				default:
					s.logger.Error("Unsupported data type",
						slog.String("type", fmt.Sprintf("%T", object)))
				}

				if err != nil {
					s.logger.Error("Error uploading blob",
						slog.Any("error", err))
				}
			}
		}()
	}

	// wait for all writers to exit
	wg.Wait()
	s.logger.Info("All blob writers exited")
}

// uploadBlob uploads a single blob to Azure Blob storage.
func (s *BlobStorageSink) uploadBlob(ctx context.Context, object *BlobStorageObject) error {
	defer func() {
		if err := object.Data.Close(); err != nil {
			s.logger.Warn("Failed to close blob storage object",
				slog.String("key", object.Key),
				slog.Any("error", err))
		}
	}()
	_, err := s.client.UploadStream(ctx, s.config.Container, object.Key, object.Data,
		s.config.UploadOptions)
	return err
}

// In returns the input channel of the BlobStorageSink connector.
func (s *BlobStorageSink) In() chan<- any {
	return s.in
}

// AwaitCompletion blocks until the BlobStorageSink connector has completed
// processing all the received data.
func (s *BlobStorageSink) AwaitCompletion() {
	<-s.done
}
