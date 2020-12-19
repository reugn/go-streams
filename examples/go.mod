module github.com/reugn/go-streams/examples

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	github.com/aerospike/aerospike-client-go v4.0.0+incompatible
	github.com/apache/pulsar-client-go v0.3.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/gorilla/websocket v1.4.2
	github.com/reugn/go-streams v0.6.0
	github.com/reugn/go-streams/aerospike v0.6.0
	github.com/reugn/go-streams/kafka v0.6.0
	github.com/reugn/go-streams/pulsar v0.6.0
	github.com/reugn/go-streams/redis v0.6.0
	github.com/reugn/go-streams/ws v0.6.0
)

replace (
	github.com/reugn/go-streams => ../
	github.com/reugn/go-streams/aerospike => ../aerospike
	github.com/reugn/go-streams/extension => ../extension
	github.com/reugn/go-streams/flow => ../flow
	github.com/reugn/go-streams/kafka => ../kafka
	github.com/reugn/go-streams/pulsar => ../pulsar
	github.com/reugn/go-streams/redis => ../redis
	github.com/reugn/go-streams/ws => ../ws
)
