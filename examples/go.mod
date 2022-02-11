module github.com/reugn/go-streams/examples

go 1.15

require (
	github.com/Shopify/sarama v1.31.1
	github.com/aerospike/aerospike-client-go/v5 v5.6.0
	github.com/apache/pulsar-client-go v0.7.0
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/gorilla/websocket v1.4.2
	github.com/nats-io/stan.go v0.10.2
	github.com/reugn/go-streams v0.7.0
	github.com/reugn/go-streams/aerospike v0.0.0
	github.com/reugn/go-streams/kafka v0.0.0
	github.com/reugn/go-streams/nats v0.0.0
	github.com/reugn/go-streams/pulsar v0.0.0
	github.com/reugn/go-streams/redis v0.0.0
	github.com/reugn/go-streams/ws v0.0.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace (
	github.com/reugn/go-streams/aerospike => ../aerospike
	github.com/reugn/go-streams/kafka => ../kafka
	github.com/reugn/go-streams/nats => ../nats
	github.com/reugn/go-streams/pulsar => ../pulsar
	github.com/reugn/go-streams/redis => ../redis
	github.com/reugn/go-streams/ws => ../ws
)
