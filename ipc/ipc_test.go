package ipc

import (
	"fmt"
	"testing"
)

type EchoServer struct {
	Server
}

func (server *EchoServer) Handle(request string, params any) *Response {
	return &Response{
		Code: "200",
		Body: "ECHO:" + request,
	}
}

func (server *EchoServer) Name() string {
	return "EchoServer"
}

func TestIpc(t *testing.T) {
	ch := make(chan string)
	server := NewIpcServer(&EchoServer{})

	client1 := NewIpcClient(server, "观察者1")
	client2 := NewIpcClient(server, "观察者2")

	resp1, _ := client1.Call("from test1", "")
	resp2, _ := client2.Call("from test2", "")

	fmt.Println("resp1:", resp1)
	fmt.Println("resp2:", resp2)

	go client1.Listen()
	go client2.Listen()
	// 发布消息
	server.Publish(Event{Data: "第一条消息"})
	server.Publish(Event{Data: "第二条消息"})

	// 取消订阅
	server.Unsubscribe(client2.Pipe)

	// 再次发布
	server.Publish(Event{Data: "第三条消息"})

	<-ch
}
