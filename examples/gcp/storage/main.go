package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
	"github.com/reugn/go-streams/flow"
	connector "github.com/reugn/go-streams/gcp"
	"google.golang.org/api/option"
)

// docker run --rm --name fake-gcs-server -p 4443:4443 -v ${PWD}/data:/data \
// fsouza/fake-gcs-server -scheme http

// curl http://0.0.0.0:4443/storage/v1/b
// curl http://0.0.0.0:4443/storage/v1/b/test-bucket/o
func main() {
	ctx := context.Background()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint("http://localhost:4443/storage/v1/"),
		option.WithoutAuthentication(),
		storage.WithJSONReads(),
	)
	if err != nil {
		log.Fatal(err)
	}

	sourceConfig := &connector.StorageSourceConfig{
		Bucket: "test-bucket",
		// Prefix: "source",
	}
	source := connector.NewStorageSource(ctx, client, sourceConfig, nil)

	mapObjects := flow.NewMap(transform, 1)

	sinkConfig := &connector.StorageSinkConfig{
		Bucket: "test-bucket",
	}
	sink := connector.NewStorageSink(ctx, client, sinkConfig, nil)

	source.
		Via(mapObjects).
		To(sink)
}

var transform = func(object *connector.StorageObject) *connector.StorageObject {
	object.Key = fmt.Sprintf("sink/%s", object.Key)
	return object
}
