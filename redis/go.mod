module github.com/reugn/go-streams/redis

go 1.15

replace github.com/reugn/go-streams => ../

require (
	github.com/go-redis/redis v6.15.9+incompatible
	github.com/onsi/ginkgo v1.14.2 // indirect
	github.com/onsi/gomega v1.10.4 // indirect
	github.com/reugn/go-streams v0.0.0-00010101000000-000000000000
)
