package main

import (
	"fmt"
	extChan "github.com/reugn/go-streams/extension"
	"github.com/reugn/go-streams/flow"
	"os"
	"os/signal"
	"syscall"
)

var keyBy1 = func(i interface{}) interface{} {
	return i.(int)
}

var keyBy2 = func(i interface{}) interface{} {
	return i.(int)
}

var coFlatMap = func(v1 interface{}, v2 interface{}, out chan<- interface{}) {
	if v1 == nil {
		return
	}
	if v2 == nil {
		return
	}
	if v1 == nil || v2 == nil {
		return
	}
	out <- (v1).(int) * (v2).(int)
}

var printData = func(r chan interface{}) {
	for i := range r {
		fmt.Printf("%d\n", i.(int))
	}
	close(r)
}

func main() {
	in1 := make(chan interface{}, 10)
	in2 := make(chan interface{}, 10)
	go func() {
		for i := 0; i <= 10; i++ {
			in1 <- i
		}
	}()
	go func() {
		for i := 0; i <= 10; i++ {
			in2 <- i
		}
	}()
	source1 := extChan.NewChanSource(in1)
	source2 := extChan.NewChanSource(in2)

	r := flow.
		KeyBy(source1, keyBy1, 10).
		Connect(flow.KeyBy(source2, keyBy2, 10), 10).
		CoFlatMap(coFlatMap, 10)
	go printData(r)
	wait()
}

func wait() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		return
	}
}
