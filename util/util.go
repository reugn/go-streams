package util

import (
	"hash/fnv"
)

// Check panics if the given error is not nil.
func Check(e error) {
	if e != nil {
		panic(e)
	}
}

// HashCode returns a uint32 hash for the given byte array.
func HashCode(b []byte) uint32 {
	h := fnv.New32a()
	_, err := h.Write(b)
	Check(err)
	return h.Sum32()
}
