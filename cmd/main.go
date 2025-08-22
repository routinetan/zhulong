// main.go
package main

import (
	"log"
	"os"
	"zhulong/app"
	"zhulong/config"
)

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	// 加载配置
	cfg, err := config.LoadConfigFromFile(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting server group with %d servers", len(cfg.Servers))

	// 创建服务器组
	mgr := app.NewServerManager(cfg.Servers)

	// 等待关闭信号
	mgr.Wait()
	log.Println("All servers stopped")
}
