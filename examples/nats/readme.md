# Nats Streaming (stan) Example

## Setup

Run `docker-compose up -d` in this directory. It will create a nats streaming server on it's own network named `nats-streaming`.

## Run

`go run main.go`

## Test

Use the `synadia/nats-box` container to check out how it works. 

In a separate terminal fire up the nats box.

`docker run --rm -it --network=nats-streaming synadia/nats-box:latest`

### Subscribe a test listener to listen for output

Once that's running, let's listen to the sink in the example which is publishing results to the topic `test2`. The `&` will make this command run in the background and we'll see the output as it comes in on the same terminal. The output might be mixed in with your other commands but don't worry, that's normal as the process is still attached to stdout and writes as it pleases =).

`stan-sub -s nats-streaming -c test-cluster -id listening output-topic &`

**Output**

You should see the listener attach to the server like below:

```
Connected to nats-streaming clusterID: [test-cluster] clientID: [listening]
Listening on [output-topic], clientID=[listening], qgroup=[] durable=[]
```


### Publish Messages

Publish messages to the `test` topic which is waiting to pump them through the stream.

`stan-pub -s nats-streaming -c test-cluster -id publisher input-topic hello`

**Output**

What you should see is the input `hello` properly modified and re-published as `HELLO*` on the second topic. The order of these might not be sequential, it depends how fast the operation happens.

```
Published [input-topic] : 'hello'
[#1] Received: sequence:1 subject:"output-topic" data:"HELLO*" timestamp:1608606737479746600
```