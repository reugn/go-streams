# go-streams
[![Build](https://github.com/reugn/go-streams/actions/workflows/build.yml/badge.svg)](https://github.com/reugn/go-streams/actions/workflows/build.yml)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/reugn/go-streams)](https://pkg.go.dev/github.com/reugn/go-streams)
[![Go Report Card](https://goreportcard.com/badge/github.com/reugn/go-streams)](https://goreportcard.com/report/github.com/reugn/go-streams)
[![codecov](https://codecov.io/gh/reugn/go-streams/branch/master/graph/badge.svg)](https://codecov.io/gh/reugn/go-streams)

A lightweight stream processing library for Go.  
`go-streams` provides a simple and concise DSL to build data pipelines.
![pipeline-architecture-example](./docs/images/pipeline-architecture-example.png)
> [Wiki](https://en.wikipedia.org/wiki/Pipeline_(computing))  
> In computing, a pipeline, also known as a data pipeline, is a set of data processing elements connected in series, where the output of one element is the input of the next one. The elements of a pipeline are often executed in parallel or in time-sliced fashion. Some amount of buffer storage is often inserted between elements.

## Overview
Building blocks:
* Source - A Source is a set of stream processing steps that has one open output.
* Flow - A Flow is a set of stream processing steps that has one open input and one open output. 
* Sink - A Sink is a set of stream processing steps that has one open input. Can be used as a Subscriber.

Flow capabilities ([flow](https://github.com/reugn/go-streams/tree/master/flow) package):  
* Map
* FlatMap
* Filter
* Reduce
* PassThrough
* Split
* FanOut
* RoundRobin
* Merge
* Flatten
* Throttler
* SlidingWindow
* TumblingWindow
* SessionWindow

Supported Connectors:
* Go channels
* File system
* Network (TCP, UDP)
* WebSocket
* [Aerospike](https://www.aerospike.com/)
* [Apache Kafka](https://kafka.apache.org/)
* [Apache Pulsar](https://pulsar.apache.org/)
* [NATS](https://nats.io/)
* [Redis](https://redis.io/)

## Examples
Usage samples are available in the examples directory.

## License
Licensed under the MIT License.
