package streams

import (
	"hash/fnv"
	"time"
)

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func NowNano() int64 {
	return time.Now().UTC().UnixNano()
}

func HashCode(b []byte) uint32 {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum32()
}
