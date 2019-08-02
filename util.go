package streams

import (
	"hash/fnv"
	"time"
)

// Check error
func Check(e error) {
	if e != nil {
		panic(e)
	}
}

// NowNano returns UnixNano in UTC
func NowNano() int64 {
	return time.Now().UTC().UnixNano()
}

// HashCode for byte array
func HashCode(b []byte) uint32 {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum32()
}
