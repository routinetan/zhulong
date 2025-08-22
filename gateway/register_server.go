package gateway

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
	"zhulong/ipc"
)

// 定义注册中心命令
const (
	RegisterCmdRegister   = 100 // 注册命令
	RegisterCmdHeartbeat  = 101 // 心跳命令
	RegisterCmdUnregister = 102 // 注销命令
	RegisterCmdGetNodes   = 103 // 获取节点列表
)

type RegisterServer struct {
	ipc.Server
	listener     net.Listener
	shutdown     chan struct{}
	wg           sync.WaitGroup // 管理工作协程
	addr         string
	gatewayNodes map[string]gatewayNode
	workerNodes  map[string]workerNode
	nodesLock    sync.RWMutex
}

type gatewayNode struct {
	ID         string
	IP         string
	Port       int
	LastActive time.Time
}

type workerNode struct {
	ID         string
	IP         string
	Port       int
	Service    string
	LastActive time.Time
}

func NewRegisterServer(addr string) *RegisterServer {
	return &RegisterServer{
		addr:         addr,
		shutdown:     make(chan struct{}),
		gatewayNodes: make(map[string]gatewayNode),
		workerNodes:  make(map[string]workerNode),
	}
}

func (rs *RegisterServer) Start() error {
	ln, err := net.Listen("tcp", rs.addr)
	if err != nil {
		return err
	}
	rs.listener = ln

	log.Printf("Register server started on %s", rs.addr)

	rs.wg.Add(1)
	go rs.acceptConnections()

	return nil
}

func (rs *RegisterServer) Stop() {
	close(rs.shutdown)
	if rs.listener != nil {
		rs.listener.Close()
	}
	rs.wg.Wait() // 等待所有协程结束
}

func (rs *RegisterServer) acceptConnections() {
	defer rs.wg.Done()

	for {
		select {
		case <-rs.shutdown:
			return
		default:
		}

		conn, err := rs.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("Register server accept error: %v", err)
			continue
		}

		rs.wg.Add(1)
		go rs.handleConnection(conn)
	}
}

func (rs *RegisterServer) handleConnection(conn net.Conn) {
	defer rs.wg.Done()
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	log.Printf("New register connection from %s", remoteAddr)

	buffer := make([]byte, 4096)

	for {
		select {
		case <-rs.shutdown:
			return
		default:
		}

		// 设置读取超时
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		n, err := conn.Read(buffer)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// 检查是否需要保持连接
				continue
			}
			log.Printf("Register connection read error from %s: %v", remoteAddr, err)
			return
		}

		// 解析消息
		msg, err := ParseWorkerMessage(bytes.NewReader(buffer[:n]))
		if err != nil {
			log.Printf("Parse register message from %s error: %v", remoteAddr, err)
			continue
		}

		// 处理消息
		rs.handleMessage(conn, msg)
	}
}

func (rs *RegisterServer) handleMessage(conn net.Conn, msg *WorkerMessage) {
	switch msg.Header.Cmd {
	case RegisterCmdRegister:
		rs.handleRegister(conn, msg.Body)
	case RegisterCmdHeartbeat:
		rs.handleHeartbeat(msg.Body)
	case RegisterCmdUnregister:
		rs.handleUnregister(msg.Body)
	case RegisterCmdGetNodes:
		rs.handleGetNodes(conn)
	default:
		log.Printf("Unknown register command: %d", msg.Header.Cmd)
	}
}

func (rs *RegisterServer) handleRegister(conn net.Conn, data []byte) {
	if len(data) < 1 {
		log.Println("Invalid register message: too short")
		return
	}

	// 节点类型 (1字节)
	nodeTypeLen := int(data[0])
	if len(data) < 1+nodeTypeLen {
		log.Println("Invalid register message: missing node type")
		return
	}
	nodeType := string(data[1 : 1+nodeTypeLen])
	data = data[1+nodeTypeLen:]

	// 节点ID (1字节)
	if len(data) < 1 {
		log.Println("Invalid register message: missing node ID length")
		return
	}
	nodeIDLen := int(data[0])
	if len(data) < 1+nodeIDLen {
		log.Println("Invalid register message: missing node ID")
		return
	}
	nodeID := string(data[1 : 1+nodeIDLen])
	data = data[1+nodeIDLen:]

	// IP (1字节)
	if len(data) < 1 {
		log.Println("Invalid register message: missing IP length")
		return
	}
	ipLen := int(data[0])
	if len(data) < 1+ipLen {
		log.Println("Invalid register message: missing IP")
		return
	}
	ip := string(data[1 : 1+ipLen])
	data = data[1+ipLen:]

	// 端口 (2字节)
	if len(data) < 2 {
		log.Println("Invalid register message: missing port")
		return
	}
	port := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]

	rs.nodesLock.Lock()
	defer rs.nodesLock.Unlock()

	now := time.Now()

	if nodeType == "gateway" {
		rs.gatewayNodes[nodeID] = gatewayNode{
			ID:         nodeID,
			IP:         ip,
			Port:       port,
			LastActive: now,
		}
		log.Printf("Gateway registered: %s@%s:%d", nodeID, ip, port)
	} else if nodeType == "worker" {
		// 服务名称 (1字节)
		if len(data) < 1 {
			log.Println("Invalid worker register: missing service length")
			return
		}
		serviceLen := int(data[0])
		if len(data) < 1+serviceLen {
			log.Println("Invalid worker register: missing service name")
			return
		}
		service := string(data[1 : 1+serviceLen])

		rs.workerNodes[nodeID] = workerNode{
			ID:         nodeID,
			IP:         ip,
			Port:       port,
			Service:    service,
			LastActive: now,
		}
		log.Printf("Worker registered: %s@%s:%d (service: %s)", nodeID, ip, port, service)
	} else {
		log.Printf("Unknown node type: %s", nodeType)
	}
}

func (rs *RegisterServer) handleHeartbeat(data []byte) {
	if len(data) < 1 {
		log.Println("Invalid heartbeat message: too short")
		return
	}

	// 节点类型 (1字节)
	nodeTypeLen := int(data[0])
	if len(data) < 1+nodeTypeLen {
		log.Println("Invalid heartbeat: missing node type")
		return
	}
	nodeType := string(data[1 : 1+nodeTypeLen])
	data = data[1+nodeTypeLen:]

	// 节点ID (1字节)
	if len(data) < 1 {
		log.Println("Invalid heartbeat: missing node ID length")
		return
	}
	nodeIDLen := int(data[0])
	if len(data) < 1+nodeIDLen {
		log.Println("Invalid heartbeat: missing node ID")
		return
	}
	nodeID := string(data[1 : 1+nodeIDLen])

	rs.nodesLock.Lock()
	defer rs.nodesLock.Unlock()

	now := time.Now()

	if nodeType == "gateway" {
		if node, exists := rs.gatewayNodes[nodeID]; exists {
			node.LastActive = now
			rs.gatewayNodes[nodeID] = node
		}
	} else if nodeType == "worker" {
		if node, exists := rs.workerNodes[nodeID]; exists {
			node.LastActive = now
			rs.workerNodes[nodeID] = node
		}
	}
}

func (rs *RegisterServer) handleUnregister(data []byte) {
	if len(data) < 1 {
		log.Println("Invalid unregister message: too short")
		return
	}

	// 节点类型 (1字节)
	nodeTypeLen := int(data[0])
	if len(data) < 1+nodeTypeLen {
		log.Println("Invalid unregister: missing node type")
		return
	}
	nodeType := string(data[1 : 1+nodeTypeLen])
	data = data[1+nodeTypeLen:]

	// 节点ID (1字节)
	if len(data) < 1 {
		log.Println("Invalid unregister: missing node ID length")
		return
	}
	nodeIDLen := int(data[0])
	if len(data) < 1+nodeIDLen {
		log.Println("Invalid unregister: missing node ID")
		return
	}
	nodeID := string(data[1 : 1+nodeIDLen])

	rs.nodesLock.Lock()
	defer rs.nodesLock.Unlock()

	if nodeType == "gateway" {
		if _, exists := rs.gatewayNodes[nodeID]; exists {
			delete(rs.gatewayNodes, nodeID)
			log.Printf("Gateway unregistered: %s", nodeID)
		}
	} else if nodeType == "worker" {
		if _, exists := rs.workerNodes[nodeID]; exists {
			delete(rs.workerNodes, nodeID)
			log.Printf("Worker unregistered: %s", nodeID)
		}
	}
}

func (rs *RegisterServer) handleGetNodes(conn net.Conn) {
	rs.nodesLock.RLock()
	defer rs.nodesLock.RUnlock()

	// 创建响应缓冲区
	buf := new(bytes.Buffer)

	// 网关节点数量 (2字节)
	binary.Write(buf, binary.BigEndian, uint16(len(rs.gatewayNodes)))

	// 写入网关节点
	for _, node := range rs.gatewayNodes {
		// 节点ID (1字节长度 + ID)
		buf.WriteByte(byte(len(node.ID)))
		buf.WriteString(node.ID)

		// IP (1字节长度 + IP)
		buf.WriteByte(byte(len(node.IP)))
		buf.WriteString(node.IP)

		// 端口 (2字节)
		binary.Write(buf, binary.BigEndian, uint16(node.Port))
	}

	// 工作节点数量 (2字节)
	binary.Write(buf, binary.BigEndian, uint16(len(rs.workerNodes)))

	// 写入工作节点
	for _, node := range rs.workerNodes {
		// 节点ID (1字节长度 + ID)
		buf.WriteByte(byte(len(node.ID)))
		buf.WriteString(node.ID)

		// IP (1字节长度 + IP)
		buf.WriteByte(byte(len(node.IP)))
		buf.WriteString(node.IP)

		// 端口 (2字节)
		binary.Write(buf, binary.BigEndian, uint16(node.Port))

		// 服务名称 (1字节长度 + 名称)
		buf.WriteByte(byte(len(node.Service)))
		buf.WriteString(node.Service)
	}

	// 创建Gateway协议消息
	msg := &WorkerMessage{
		Header: WorkerHeader{
			Cmd: RegisterCmdGetNodes,
		},
		Body: buf.Bytes(),
	}

	// 发送消息
	if _, err := conn.Write(EncodeWorkerMessage(msg)); err != nil {
		log.Printf("Send nodes list error: %v", err)
	}
}

// 获取节点统计信息
func (rs *RegisterServer) GetStats() (int, int) {
	rs.nodesLock.RLock()
	defer rs.nodesLock.RUnlock()
	return len(rs.gatewayNodes), len(rs.workerNodes)
}

// 获取本地IP地址
func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}

	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	return "127.0.0.1"
}

// 在 register_server.go 中补充 CleanExpiredNodesLoop 方法
func (rs *RegisterServer) CleanExpiredNodesLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rs.CleanExpiredNodes()
		case <-rs.shutdown:
			return
		}
	}
}

func (rs *RegisterServer) CleanExpiredNodes() {
	rs.nodesLock.Lock()
	defer rs.nodesLock.Unlock()

	now := time.Now()
	expiredCount := 0

	// 清理过期的网关节点
	for id, node := range rs.gatewayNodes {
		if now.Sub(node.LastActive) > 90*time.Second { // 超时90秒
			delete(rs.gatewayNodes, id)
			expiredCount++
		}
	}

	// 清理过期的工作节点
	for id, node := range rs.workerNodes {
		if now.Sub(node.LastActive) > 90*time.Second { // 超时90秒
			delete(rs.workerNodes, id)
			expiredCount++
		}
	}

	if expiredCount > 0 {
		log.Printf("Cleaned %d expired nodes", expiredCount)
	}
}
