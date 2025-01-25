package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/azure"
	"github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

const (
	azuriteAccountName = "devstoreaccount1"
	azuriteAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

	testServiceAddress = "http://127.0.0.1:10000/devstoreaccount1"
	testContainerName  = "container1"
)

// Use the Azurite emulator for local Azure Blob storage environment.
//
//	docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \
//			azurite-blob --blobHost 0.0.0.0 --blobPort 10000
func main() {
	ctx := context.Background()
	client := newBlobClient()

	if _, err := client.CreateContainer(ctx, testContainerName, nil); err != nil {
		log.Print(err)
	}

	// load blobs from a channel source
	newChanSource().
		Via(toBlobStorageFlow()).
		To(newBlobSink(ctx, client))

	// read blob data to stdout
	newBlobSource(ctx, client).
		Via(readBlobStorageFlow()).
		To(extension.NewStdoutSink())

	// clean up the container
	cleanUp(ctx, client)
}

func newBlobClient() *azblob.Client {
	cred, err := azblob.NewSharedKeyCredential(azuriteAccountName, azuriteAccountKey)
	if err != nil {
		log.Fatal(err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func newBlobSource(ctx context.Context, client *azblob.Client) streams.Source {
	return azure.NewBlobStorageSource(ctx, client, &azure.BlobStorageSourceConfig{
		Container: testContainerName,
	}, nil)
}

func newBlobSink(ctx context.Context, client *azblob.Client) streams.Sink {
	return azure.NewBlobStorageSink(ctx, client, &azure.BlobStorageSinkConfig{
		Container: testContainerName,
	}, nil)
}

func newChanSource() streams.Source {
	data := []string{"aa", "b", "c"}
	inputCh := make(chan any, 3)
	for _, in := range data {
		inputCh <- in
	}
	close(inputCh)
	return extension.NewChanSource(inputCh)
}

func toBlobStorageFlow() streams.Flow {
	return flow.NewMap(func(data string) *azure.BlobStorageObject {
		return &azure.BlobStorageObject{
			Key:  getKey(data),
			Data: io.NopCloser(strings.NewReader(data)),
		}
	}, 1)
}

func getKey(value string) string {
	if len(value) > 1 {
		// add a subfolder
		return fmt.Sprintf("%s/%s.txt", value, value)
	}
	return fmt.Sprintf("%s.txt", value)
}

func readBlobStorageFlow() streams.Flow {
	return flow.NewMap(func(blob *azure.BlobStorageObject) string {
		defer blob.Data.Close()
		data, err := io.ReadAll(blob.Data)
		if err != nil {
			log.Fatal(err)
		}
		return strings.ToUpper(string(data))
	}, 1)
}

func cleanUp(ctx context.Context, client *azblob.Client) {
	log.Print("Deleting blobs...")

	newBlobSource(ctx, client).
		Via(flow.NewMap(func(blob *azure.BlobStorageObject) string {
			if _, err := client.DeleteBlob(ctx, testContainerName, blob.Key, nil); err != nil {
				log.Fatal(err)
			}
			return blob.Key
		}, 1)).
		To(extension.NewStdoutSink())
}
