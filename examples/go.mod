module github.com/reugn/go-streams/examples

go 1.15

require (
	github.com/Shopify/sarama v1.33.0
	github.com/aerospike/aerospike-client-go/v5 v5.6.0
	github.com/apache/pulsar-client-go v0.7.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/gorilla/websocket v1.4.2
	github.com/nats-io/nats.go v1.15.0
	github.com/nats-io/stan.go v0.10.2
	github.com/reugn/go-streams v0.7.0
	github.com/reugn/go-streams/aerospike v0.0.0
	github.com/reugn/go-streams/kafka v0.0.0
	github.com/reugn/go-streams/nats v0.0.0
	github.com/reugn/go-streams/pulsar v0.0.0
	github.com/reugn/go-streams/redis v0.0.0
	github.com/reugn/go-streams/ws v0.0.0
)

replace (
	github.com/reugn/go-streams => ../
	github.com/reugn/go-streams/aerospike => ../aerospike
	github.com/reugn/go-streams/kafka => ../kafka
	github.com/reugn/go-streams/nats => ../nats
	github.com/reugn/go-streams/pulsar => ../pulsar
	github.com/reugn/go-streams/redis => ../redis
	github.com/reugn/go-streams/ws => ../ws
)
