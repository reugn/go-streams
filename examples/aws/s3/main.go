package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	connector "github.com/reugn/go-streams/aws"
	"github.com/reugn/go-streams/flow"
)

func main() {
	ctx := context.Background()
	client, err := newS3Client(ctx)
	if err != nil {
		log.Fatal(err)
	}

	sourceConfig := &connector.S3SourceConfig{
		Bucket: "stream-test",
		Path:   "source",
	}
	source := connector.NewS3Source(ctx, client, sourceConfig, nil)

	mapObjects := flow.NewMap(transform, 1)

	sinkConfig := &connector.S3SinkConfig{
		Bucket: "stream-test",
	}
	sink := connector.NewS3Sink(ctx, client, sinkConfig, nil)

	source.
		Via(mapObjects).
		To(sink)
}

func newS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile("minio"),
		config.WithRegion("us-east-1"),
	)

	if err != nil {
		return nil, fmt.Errorf("unable to load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:9000")
		o.UsePathStyle = true
	})

	return client, nil
}

var transform = func(object *connector.S3Object) *connector.S3Object {
	object.Key = strings.ReplaceAll(object.Key, "source", "sink")
	return object
}
