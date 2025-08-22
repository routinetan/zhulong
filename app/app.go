package app

import (
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"zhulong/config"
	"zhulong/factory"
)

type ServerManager struct {
	instances []*factory.ServerInstance
	processes []*os.Process
}

func NewServerManager(configs []config.ServerConfig) *ServerManager {
	mgr := &ServerManager{}
	for _, cfg := range configs {
		if !cfg.Enabled {
			continue
		}

		if cfg.AsIndependent {
			mgr.startIndependentServer(cfg)
		} else {
			mgr.startEmbeddedServer(cfg)
		}
	}
	return mgr
}

func (m *ServerManager) startEmbeddedServer(cfg config.ServerConfig) {
	instance, err := factory.CreateServer(cfg)
	if err != nil {
		log.Printf("Failed to create %s server: %v", cfg.Type, err)
		return
	}

	go func() {
		if err := instance.Server.Start(); err != nil {
			log.Printf("Failed to start %s server: %v", cfg.Type, err)
		}
	}()

	m.instances = append(m.instances, instance)
}

func (m *ServerManager) startIndependentServer(cfg config.ServerConfig) {
	// 构建命令行参数
	args := buildCommandArgs(cfg)

	// 启动独立进程
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Printf("Failed to start independent %s server: %v", cfg.Type, err)
		return
	}

	m.processes = append(m.processes, cmd.Process)
	log.Printf("Started independent %s server (PID: %d)", cfg.Type, cmd.Process.Pid)
}

func buildCommandArgs(cfg config.ServerConfig) []string {
	binName := ""
	switch cfg.Type {
	case config.ServerTypeRegister:
		binName = "./register_server"
	case config.ServerTypeGateway:
		binName = "./gateway_server"
	case config.ServerTypeWorker:
		binName = "./worker_server"
	}

	args := []string{
		binName,
		"--name", cfg.Name,
		"--addr", cfg.Address,
		"--p2p-addr", cfg.P2PAddress,
	}

	for _, peer := range cfg.Bootstrap {
		args = append(args, "--bootstrap", peer)
	}

	if cfg.Type == config.ServerTypeWorker {
		args = append(args, "--service", cfg.Service)
	}

	if cfg.Protocol != "" {
		args = append(args, "--protocol", cfg.Protocol)
	}

	return []string{}
}

func (m *ServerManager) Stop() {
	// 停止嵌入式服务器
	for _, instance := range m.instances {
		instance.Server.Stop()
	}

	// 停止独立进程
	for _, proc := range m.processes {
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			log.Printf("Failed to send SIGTERM to PID %d: %v", proc.Pid, err)
		}
	}

	// 等待所有进程退出
	for _, proc := range m.processes {
		proc.Wait()
	}
}

func (m *ServerManager) Wait() {
	// 创建信号通道
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号
	<-sigCh
	log.Println("Received shutdown signal")
	m.Stop()
}
