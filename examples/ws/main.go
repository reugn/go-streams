package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/reugn/go-streams"
	ext "github.com/reugn/go-streams/extension/ws"
	"github.com/reugn/go-streams/flow"

	"github.com/gorilla/websocket"
)

type wsServer struct {
	clients   map[*websocket.Conn]bool
	broadcast chan interface{}
	upgrader  websocket.Upgrader
}

func startWsServer() {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	server := &wsServer{
		clients:   make(map[*websocket.Conn]bool),
		broadcast: make(chan interface{}),
		upgrader:  upgrader,
	}

	go server.init()
}

func (server *wsServer) init() {
	http.HandleFunc("/ws", server.handleConnections)
	go server.handleMessages()

	// send initial message
	timer := time.NewTimer(time.Second)
	go func() {
		select {
		case <-timer.C:
			payload := []byte("foo")
			server.broadcast <- ext.Message{
				MsgType: websocket.TextMessage,
				Payload: payload,
			}
		}
	}()

	log.Print("http server started on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("http.ListAndServe: ", err)
	}
}

func (server *wsServer) handleConnections(w http.ResponseWriter, r *http.Request) {
	conn, err := server.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	server.clients[conn] = true

	for {
		t, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error on ReadMessage: %v", err)
			delete(server.clients, conn)
			break
		}

		m := ext.Message{
			MsgType: t,
			Payload: msg,
		}

		log.Printf("Broadcasting message: %s", string(m.Payload))
		time.Sleep(time.Second)

		server.broadcast <- m
	}
}

func (server *wsServer) handleMessages() {
	for {
		msg := <-server.broadcast
		for client := range server.clients {
			m := msg.(ext.Message)
			err := client.WriteMessage(m.MsgType, m.Payload)
			if err != nil {
				log.Printf("Error on WriteMessage: %v", err)
				client.Close()
				delete(server.clients, client)
			}
		}
	}
}

func main() {
	ctx, cancelFunc := context.WithCancel(context.Background())

	timer := time.NewTimer(time.Second * 10)
	go func() {
		select {
		case <-timer.C:
			log.Print("ctx")
			cancelFunc()
		}
	}()

	go term()
	startWsServer()
	time.Sleep(time.Millisecond * 500)

	url := "ws://127.0.0.1:8080/ws"
	source, err := ext.NewWebSocketSource(ctx, url)
	streams.Check(err)
	flow1 := flow.NewMap(appendAsterix, 1)
	sink, err := ext.NewWebSocketSink(ctx, url)
	streams.Check(err)

	source.Via(flow1).To(sink)

	log.Print("Exiting...")
}

var appendAsterix = func(in interface{}) interface{} {
	msg := in.(ext.Message)
	return string(msg.Payload) + "*"
}

func term() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-c:
		os.Exit(1)
	}
}
