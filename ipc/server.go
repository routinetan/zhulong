package ipc

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Request struct {
	Method string
	Params any
}

type Response struct {
	Code string
	Body string
}

// Observer 观察者接口
type Observer interface {
	Update(msg string)
}

type Server interface {
	Name() string
	Handle(method string, params any) *Response
}

type IpcServer struct {
	Server
	observers map[chan Event]struct{}
	mu        sync.RWMutex
}

func NewIpcServer(server Server) *IpcServer {
	return &IpcServer{Server: server, mu: sync.RWMutex{}, observers: make(map[chan Event]struct{})}
}

func (server *IpcServer) Connect() chan string {
	session := make(chan string, 0)
	go func(c chan string) {
		for {
			request := <-c
			if request == "CLOSE" {
				break
			}
			var req Request
			err := json.Unmarshal([]byte(request), &req)
			if err != nil {
				fmt.Println("Invalid request format:", request)
			}
			resp := server.Handle(req.Method, req.Params)

			b, err := json.Marshal(resp)
			c <- string(b)
		}
	}(session)
	fmt.Println("A new session has been created successfully.")
	return session
}

type Event struct {
	Data string
}

func (server *IpcServer) Subscribe() chan Event {
	ch := make(chan Event, 10) // 带缓冲的通道
	server.mu.Lock()
	server.observers[ch] = struct{}{}
	server.mu.Unlock()
	return ch
}

func (server *IpcServer) Unsubscribe(ch chan Event) {
	server.mu.Lock()
	defer server.mu.Unlock()
	delete(server.observers, ch)
	close(ch)
}

func (server *IpcServer) Publish(event Event) {
	server.mu.RLock()
	defer server.mu.RUnlock()
	for observer := range server.observers {
		select {
		case observer <- event:
		default:
			fmt.Println("通道已满，消息丢弃")
		}
	}
}
