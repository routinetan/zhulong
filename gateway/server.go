package gateway

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
	"zhulong/common"
	"zhulong/ipc"
)

type GatewayServer struct {
	ipc.Server
	Listener     net.Listener
	Protocol     common.Protocol
	ConnMgr      *ConnectionManager
	workerAddr   string
	workerConn   net.Conn
	workerLock   sync.Mutex
	shutdownChan chan struct{}
}

func NewGatewayServer(addr string, protocol common.Protocol) (*GatewayServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &GatewayServer{
		Listener:     ln,
		Protocol:     protocol,
		ConnMgr:      NewConnectionManager(),
		shutdownChan: make(chan struct{}),
	}, nil
}

func (gs *GatewayServer) SetWorkerAddress(addr string) {
	gs.workerAddr = addr
}

func (gs *GatewayServer) Start() error {
	log.Printf("Gateway server started on %s", gs.Listener.Addr())

	// 连接业务工作者
	go gs.connectToWorker()

	// 接受客户端连接
	go gs.acceptClients()

	return nil
}

func (gs *GatewayServer) Stop() {
	close(gs.shutdownChan)

	// 关闭监听器
	if gs.Listener != nil {
		gs.Listener.Close()
	}

	// 关闭工作连接
	gs.workerLock.Lock()
	if gs.workerConn != nil {
		gs.workerConn.Close()
	}
	gs.workerLock.Unlock()

	// 关闭所有客户端连接
	gs.ConnMgr.CloseAll()
}

func (gs *GatewayServer) acceptClients() {
	for {
		select {
		case <-gs.shutdownChan:
			return
		default:
		}

		conn, err := gs.Listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("Accept error: %v", err)
			continue
		}

		go gs.handleClientConnection(conn)
	}
}

func (gs *GatewayServer) connectToWorker() {
	for {
		select {
		case <-gs.shutdownChan:
			return
		default:
		}

		conn, err := net.Dial("tcp", gs.workerAddr)
		if err != nil {
			log.Printf("Failed to connect to worker: %v. Retrying in 3 seconds...", err)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("Connected to business worker at %s", gs.workerAddr)

		gs.workerLock.Lock()
		gs.workerConn = conn
		gs.workerLock.Unlock()

		// 处理来自worker的消息
		go gs.handleWorkerConnection(conn)

		return
	}
}

// 修改handleClientConnection方法
func (gs *GatewayServer) handleClientConnection(conn net.Conn) {
	defer conn.Close()

	// 添加到连接管理器
	client := gs.ConnMgr.AddClient(conn)
	defer gs.ConnMgr.RemoveClient(client.ID)

	// 创建协议解析器
	parser := gs.Protocol

	// 读缓冲区
	buffer := make([]byte, 4096)
	readBuffer := bytes.NewBuffer(nil)

	for {
		// 设置读超时
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		n, err := conn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// 检查心跳超时
				//if time.Since(gs.ConnMgr.GetClient(client.ID).LastActive) > 60*time.Second {
				//	return
				//}
				continue
			}
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			return
		}

		// 更新活跃时间
		gs.ConnMgr.UpdateActivity(client.ID)

		// 添加到读缓冲区
		readBuffer.Write(buffer[:n])

		// 解析数据包
		for {
			packetLen, err := parser.Input(readBuffer.Bytes())
			if err != nil {
				log.Printf("Protocol input error: %v", err)
				return
			}

			if packetLen == 0 {
				break // 数据不足
			}

			if readBuffer.Len() < packetLen {
				break // 数据不足
			}

			packet := readBuffer.Next(packetLen)
			data, err := parser.Decode(packet)
			if err != nil {
				log.Printf("Protocol decode error: %v", err)
				continue
			}

			// 转发给业务进程
			gs.forwardToWorker(client.ID, data.([]byte))
		}
	}
}

func (gs *GatewayServer) handleWorkerConnection(conn net.Conn) {
	//defer func() {
	//	conn.Close()
	//	gs.workerLock.Lock()
	//	gs.workerConn = nil
	//	gs.workerLock.Unlock()
	//
	//	// 尝试重新连接
	//	if !gs.isShuttingDown() {
	//		log.Println("Worker connection lost, reconnecting...")
	//		go gs.connectToWorker()
	//	}
	//}()
	//
	//router := NewWorkerRouter(gs.ConnMgr)
	//for {
	//	msg, err := ParseWorkerMessage(conn)
	//	if err != nil {
	//		if gs.isShuttingDown() {
	//			return
	//		}
	//		if errors.Is(err, net.ErrClosed) {
	//			log.Println("Worker connection closed")
	//			return
	//		}
	//		log.Printf("Parse worker message error: %v", err)
	//		return
	//	}
	//
	//	router.RouteMessage(msg)
	//}
}

func (gs *GatewayServer) isShuttingDown() bool {
	select {
	case <-gs.shutdownChan:
		return true
	default:
		return false
	}
}

// 转发消息到业务进程
func (gs *GatewayServer) forwardToWorker(connID string, data []byte) {
	gs.workerLock.Lock()
	defer gs.workerLock.Unlock()

	if gs.workerConn == nil {
		log.Println("No worker connection available")
		return
	}

	msg := &WorkerMessage{
		ConnID: connID,
		Header: WorkerHeader{
			Cmd: common.CmdNewMessage,
		},
		Body: data,
	}

	if _, err := gs.workerConn.Write(EncodeWorkerMessage(msg)); err != nil {
		log.Printf("Failed to send message to worker: %v", err)
	}
}
