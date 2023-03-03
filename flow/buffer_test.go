package flow_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
)

// TestNewBuffer ...
func TestNewBuffer(t *testing.T) {
	randTime := 1 + rand.Intn(15)
	ch := make(chan any)
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(time.Millisecond * time.Duration(randTime))
			ch <- i
		}
		close(ch)
	}()
	source := extension.NewChanSource(ch)

	result := sync.Map{}
	pTime := time.Now()

	// m collects grouped results after calling `NewBuffer`
	m := flow.NewMap(func(i []int) interface{} {
		if len(i) > 6 {
			t.Fatalf("%v overflow the size", len(i))
		}
		fmt.Printf("buffer data array:%v, time consuming:%v\n", i, time.Since(pTime))
		pTime = time.Now()
		for _, v := range i {
			_, ok := result.Load(v)
			if ok {
				t.Fatalf("found duplicate data:%v", v)
			}
			result.Store(v, "")
		}
		return i
	}, 1)

	source.Via(flow.NewBuffer[int](time.Millisecond*100, 6, 4)).Via(m).To(extension.NewIgnoreSink())

	// All data coming from the source will go through `m`
	for i := 0; i < 100; i++ {
		_, ok := result.Load(i)
		if !ok {
			t.Fatalf("cannot find %v", i)
		}
	}
}
