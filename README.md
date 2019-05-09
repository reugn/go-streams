# go-streams
Go stream processing library.  
Provides simple and concise DSL to build data pipelines.
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
* Kafka

## Examples
Could be found in the examples directory.

## License
Licensed under the MIT License.