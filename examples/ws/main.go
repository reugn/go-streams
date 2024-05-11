package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/reugn/go-streams/flow"
	ext "github.com/reugn/go-streams/ws"
)

type wsServer struct {
	clients   map[*websocket.Conn]bool
	broadcast chan any
	upgrader  websocket.Upgrader
}

func startWsServer() {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(_ *http.Request) bool {
			return true
		},
	}
	server := &wsServer{
		clients:   make(map[*websocket.Conn]bool),
		broadcast: make(chan any),
		upgrader:  upgrader,
	}

	go server.init()
}

func (server *wsServer) init() {
	http.HandleFunc("/ws", server.handleConnections)
	go server.handleMessages()

	// send initial message
	go func() {
		<-time.After(time.Second)
		payload := []byte("foo")
		server.broadcast <- ext.Message{
			MsgType: websocket.TextMessage,
			Payload: payload,
		}
	}()

	log.Print("http server started on :8080")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatalf("Error in http.ListAndServe: %s", err)
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
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			log.Printf("Error in ReadMessage: %s", err)
			delete(server.clients, conn)
			break
		}

		wsMessage := ext.Message{
			MsgType: messageType,
			Payload: payload,
		}

		log.Printf("Broadcasting message: %s", string(wsMessage.Payload))
		time.Sleep(time.Second)

		server.broadcast <- wsMessage
	}
}

func (server *wsServer) handleMessages() {
	for {
		msg := <-server.broadcast
		for client := range server.clients {
			m := msg.(ext.Message)
			err := client.WriteMessage(m.MsgType, m.Payload)
			if err != nil {
				log.Printf("Error in WriteMessage: %s", err)
				// close the client and remove it from the list
				client.Close()
				delete(server.clients, client)
			}
		}
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go term()
	startWsServer()
	time.Sleep(500 * time.Millisecond)

	url := "ws://127.0.0.1:8080/ws"
	source, err := ext.NewWebSocketSource(ctx, url)
	if err != nil {
		log.Fatal(err)
	}

	addAsteriskMapFlow := flow.NewMap(addAsterisk, 1)
	sink, err := ext.NewWebSocketSink(ctx, url)
	if err != nil {
		log.Fatal(err)
	}

	source.
		Via(addAsteriskMapFlow).
		To(sink)
}

var addAsterisk = func(msg ext.Message) string {
	return string(msg.Payload) + "*"
}

func term() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	os.Exit(1)
}
