# go-streams
[![Build](https://github.com/reugn/go-streams/actions/workflows/build.yml/badge.svg)](https://github.com/reugn/go-streams/actions/workflows/build.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/reugn/go-streams)](https://pkg.go.dev/github.com/reugn/go-streams)
[![Go Report Card](https://goreportcard.com/badge/github.com/reugn/go-streams)](https://goreportcard.com/report/github.com/reugn/go-streams)
[![codecov](https://codecov.io/gh/reugn/go-streams/branch/master/graph/badge.svg)](https://codecov.io/gh/reugn/go-streams)

`go-streams` provides a lightweight and efficient stream processing framework for Go. Its concise DSL allows
for easy definition of declarative data pipelines using composable sources, flows, and sinks.

![pipeline-architecture-example](./docs/images/pipeline-architecture-example.png)

> [Wiki](https://en.wikipedia.org/wiki/Pipeline_(computing))  
> In computing, a pipeline, also known as a data pipeline, is a set of data processing elements connected in series,
> where the output of one element is the input of the next one. The elements of a pipeline are often executed in
> parallel or in time-sliced fashion. Some amount of buffer storage is often inserted between elements.

## Overview
The core module has no external dependencies and provides three key [components](./streams.go)
for constructing stream processing pipelines:

- **Source:** The entry point of a pipeline, emitting data into the stream. (One open output)
- **Flow:** A processing unit, transforming data as it moves through the pipeline. (One open input, one open output)
- **Sink:** The termination point of a pipeline, consuming processed data and often acting as a subscriber. (One
  open input)

### Flows
The [flow](flow) package provides a collection of `Flow` implementations for common stream
processing operations. These building blocks can be used to transform and manipulate data within pipelines.

- **Map:** Transforms each element in the stream.
- **FlatMap:** Transforms each element into a stream of slices of zero or more elements.
- **Filter:** Selects elements from the stream based on a condition.
- **Reduce:** Combines elements of the stream with the last reduced value and emits the new value.
- **PassThrough:** Passes elements through unchanged.
- **Split<sup>1</sup>:** Divides the stream into two streams based on a boolean predicate.
- **FanOut<sup>1</sup>:** Duplicates the stream to multiple outputs for parallel processing.
- **RoundRobin<sup>1</sup>:** Distributes elements evenly across multiple outputs.
- **Merge<sup>1</sup>:** Combines multiple streams into a single stream.
- **ZipWith<sup>1</sup>:** Combines elements from multiple streams using a function.
- **Flatten<sup>1</sup>:** Flattens a stream of slices of elements into a stream of elements.
- **Batch:** Breaks a stream of elements into batches based on size or timing.
- **Throttler:** Limits the rate at which elements are processed.
- **SlidingWindow:** Creates overlapping windows of elements.
- **TumblingWindow:** Creates non-overlapping, fixed-size windows of elements.
- **SessionWindow:** Creates windows based on periods of activity and inactivity.
- **Keyed:** Groups elements by key for parallel processing of related data.

**<sup>1</sup>** Utility Flows

### Connectors
Standard `Source` and `Sink` implementations are located in the [extension](extension) package.

* Go channel inbound and outbound connector
* File inbound and outbound connector
* Standard Go `io.Reader` Source and `io.Writer` Sink connectors
* `os.Stdout` and `Discard` Sink connectors (useful for development and debugging)

The following connectors are available as separate modules and have their own dependencies.
* [Aerospike](https://www.aerospike.com/)
* [Apache Kafka](https://kafka.apache.org/)
* [Apache Pulsar](https://pulsar.apache.org/)
* AWS ([S3](https://aws.amazon.com/s3/))
* Azure ([Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/))
* GCP ([Storage](https://cloud.google.com/storage/))
* [NATS](https://nats.io/)
* [Redis](https://redis.io/)
* [WebSocket](https://en.wikipedia.org/wiki/WebSocket)

## Usage Examples
See the [examples](examples) directory for practical code samples demonstrating how to build
complete stream processing pipelines, covering various use cases and integration scenarios.

## License
Licensed under the MIT License.
