// common/server.go
package common

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ServerRunnable interface {
	Start() error
	Stop()
}

// ServerType 定义服务器类型
type ServerType int

const (
	ServerTypeGateway ServerType = iota
	ServerTypeRegister
	ServerTypeWorker
)

// ConnHandler 连接处理器接口
type ConnHandler interface {
	HandleConn(conn net.Conn)
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Type        ServerType
	Name        string
	Address     string
	ConnHandler ConnHandler
	OnStart     func()
	Context     context.Context
	OnShutdown  func()
}

// Server 通用服务器结构体
type Server struct {
	Config         *ServerConfig
	listener       net.Listener
	connections    map[net.Conn]struct{}
	connLock       sync.RWMutex
	wg             sync.WaitGroup
	Shutdown       chan struct{}
	StartTime      time.Time
	Context        context.Context
	maxConnections int
	running        bool
}

// NewServer 创建新的服务器实例
func NewServer(config *ServerConfig) (*Server, error) {
	if config.Address == "" {
		return nil, errors.New("server address is required")
	}

	if config.ConnHandler == nil {
		return nil, errors.New("connection handler is required")
	}

	return &Server{
		Config:      config,
		connections: make(map[net.Conn]struct{}),
		Shutdown:    make(chan struct{}),
	}, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.Config.Address)
	if err != nil {
		return err
	}
	s.listener = ln
	s.running = true

	log.Printf("[%s] Server started on %s", s.Config.Name, s.Config.Address)

	// 调用启动回调
	if s.Config.OnStart != nil {
		s.Config.OnStart()
	}

	// 启动连接接受协程
	s.wg.Add(1)
	go s.acceptConnections()

	// 启动优雅关闭监听
	s.wg.Add(1)
	go s.listenForShutdown()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop() {
	if !s.running {
		return
	}

	close(s.Shutdown)
	s.running = false

	// 关闭监听器
	if s.listener != nil {
		s.listener.Close()
	}

	// 关闭所有活动连接
	s.connLock.Lock()
	for conn := range s.connections {
		conn.Close()
		delete(s.connections, conn)
	}
	s.connLock.Unlock()

	// 调用关闭回调
	if s.Config.OnShutdown != nil {
		s.Config.OnShutdown()
	}

	// 等待所有协程完成
	s.wg.Wait()
	log.Printf("[%s] Server stopped", s.Config.Name)
}

// acceptConnections 接受连接
func (s *Server) acceptConnections() {
	defer s.wg.Done()

	for {
		select {
		case <-s.Shutdown:
			return
		default:
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("Accept error: %v", err)
			continue
		}

		s.connLock.Lock()
		s.connections[conn] = struct{}{}
		s.connLock.Unlock()

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection 处理连接
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()

		s.connLock.Lock()
		delete(s.connections, conn)
		s.connLock.Unlock()

		s.wg.Done()
	}()

	s.Config.ConnHandler.HandleConn(conn)
}

// listenForShutdown 监听关闭信号
func (s *Server) listenForShutdown() {
	defer s.wg.Done()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Printf("[%s] Received shutdown signal", s.Config.Name)
		s.Stop()
	case <-s.Shutdown:
		// 内部关闭
	}
}

// GetActiveConnections 获取当前活动连接数
func (s *Server) GetActiveConnections() int {
	s.connLock.RLock()
	defer s.connLock.RUnlock()
	return len(s.connections)
}

// IsRunning 检查服务器是否在运行
func (s *Server) IsRunning() bool {
	return s.running
}

// WaitUntilShutdown 等待服务器关闭
func (s *Server) WaitUntilShutdown() {
	// 创建超时上下文
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 等待关闭完成
	select {
	case <-ctx.Done():
		log.Printf("[%s] Shutdown timed out", s.Config.Name)
	case <-s.Shutdown:
		// 正常关闭
	}
}

// 在通用 Server 中添加连接管理优化
func (s *Server) AddConnection(conn net.Conn) {
	s.connLock.Lock()
	s.connections[conn] = struct{}{}
	s.connLock.Unlock()
}

func (s *Server) RemoveConnection(conn net.Conn) {
	s.connLock.Lock()
	delete(s.connections, conn)
	s.connLock.Unlock()
}

func (s *Server) CloseConnection(conn net.Conn) {
	s.RemoveConnection(conn)
	conn.Close()
}

// 在通用 Server 中添加监控支持
func (s *Server) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"name":           s.Config.Name,
		"type":           s.Config.Type.String(),
		"address":        s.Config.Address,
		"running":        s.running,
		"connections":    s.GetActiveConnections(),
		"start_time":     s.StartTime,
		"uptime_seconds": time.Since(s.StartTime).Seconds(),
	}
}

func (t ServerType) String() string {
	switch t {
	case ServerTypeGateway:
		return "Gateway"
	case ServerTypeRegister:
		return "Register"
	case ServerTypeWorker:
		return "Worker"
	default:
		return "Unknown"
	}
}

// 在通用 Server 中添加连接限制
func (s *Server) StartWithMaxConnections(max int) error {
	if max <= 0 {
		return errors.New("max connections must be greater than 0")
	}

	s.maxConnections = max
	return s.Start()
}
