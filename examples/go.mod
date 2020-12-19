module github.com/reugn/go-streams/examples

go 1.15

require (
	github.com/Shopify/sarama v1.27.2
	github.com/aerospike/aerospike-client-go v4.0.0+incompatible
	github.com/apache/pulsar-client-go v0.3.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/gorilla/websocket v1.4.2
	github.com/reugn/go-streams v0.0.0-00010101000000-000000000000
	github.com/reugn/go-streams/extension/aerospike v0.0.0-00010101000000-000000000000
	github.com/reugn/go-streams/extension/kafka v0.0.0-00010101000000-000000000000
	github.com/reugn/go-streams/extension/pulsar v0.0.0-00010101000000-000000000000
	github.com/reugn/go-streams/extension/redis v0.0.0-00010101000000-000000000000
	github.com/reugn/go-streams/extension/ws v0.0.0-00010101000000-000000000000
)

replace (
	github.com/reugn/go-streams => ../
	github.com/reugn/go-streams/extension => ../extension
	github.com/reugn/go-streams/extension/aerospike => ../aerospike
	github.com/reugn/go-streams/extension/kafka => ../kafka
	github.com/reugn/go-streams/extension/pulsar => ../pulsar
	github.com/reugn/go-streams/extension/redis => ../redis
	github.com/reugn/go-streams/extension/ws => ../ws
	github.com/reugn/go-streams/flow => ../flow
)
