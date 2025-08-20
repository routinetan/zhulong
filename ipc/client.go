package ipc

import (
	"encoding/json"
	"fmt"
)

type IpcClient struct {
	conn chan string
	Pipe chan Event
	Name string
}

func NewIpcClient(server *IpcServer, name string) *IpcClient {
	c := server.Connect()
	pipe := server.Subscribe()
	return &IpcClient{conn: c, Name: name, Pipe: pipe}
}

func (client *IpcClient) Call(method string, params any) (resp *Response, err error) {
	req := Request{method, params}
	b, err := json.Marshal(req)
	if err != nil {
		return
	}
	client.conn <- string(b)
	str := <-client.conn
	var resp1 Response
	json.Unmarshal([]byte(str), &resp1)
	resp = &resp1
	return
}

func (client *IpcClient) Listen() {
	for event := range client.Pipe {
		fmt.Printf("观察者%s 收到: %s\n", client.Name, event.Data)
	}
}
