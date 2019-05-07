package streams

import "time"

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func NowNano() int64 {
	return time.Now().UTC().UnixNano()
}
