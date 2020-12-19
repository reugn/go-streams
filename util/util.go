package util

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

// HashCode for the byte array
func HashCode(b []byte) uint32 {
	h := fnv.New32a()
	_, err := h.Write(b)
	Check(err)
	return h.Sum32()
}
