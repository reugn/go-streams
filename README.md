# go-streams
[![Build Status](https://travis-ci.org/reugn/go-streams.svg?branch=master)](https://travis-ci.org/reugn/go-streams)
[![GoDoc](https://godoc.org/github.com/reugn/go-streams?status.svg)](https://godoc.org/github.com/reugn/go-streams)
[![Go Report Card](https://goreportcard.com/badge/github.com/reugn/go-streams)](https://goreportcard.com/report/github.com/reugn/go-streams)
[![codecov](https://codecov.io/gh/reugn/go-streams/branch/master/graph/badge.svg)](https://codecov.io/gh/reugn/go-streams)

Go stream processing library.  
Provides simple and concise DSL to build data pipelines.
![pipeline-architecture-example](./images/pipeline-architecture-example.png)
> [Wiki](https://en.wikipedia.org/wiki/Pipeline_(computing))  
> In computing, a pipeline, also known as a data pipeline,[1] is a set of data processing elements connected in series, where the output of one element is the input of the next one. The elements of a pipeline are often executed in parallel or in time-sliced fashion. Some amount of buffer storage is often inserted between elements.

## Overview
Building blocks:
* Source - A Source is a set of stream processing steps that has one open output.
* Flow - A Flow is a set of stream processing steps that has one open input and one open output. 
* Sink - A Sink is a set of stream processing steps that has one open input. Can be used as a Subscriber.

Flow capabilities (flow package):  
* Map
* FlatMap
* Filter
* PassThrough
* Split
* FanOut
* Merge
* Throttler
* SlidingWindow
* TumblingWindow

Supported Sources and Sinks (ext package):
* Go channels
* File system
* [Kafka](https://kafka.apache.org/)
* [Redis](https://redis.io/)

## Examples
Could be found in the examples directory.

## License
Licensed under the MIT License.