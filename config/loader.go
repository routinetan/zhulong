// config/loader.go
package config

import (
	"errors"
	"flag"
	"gopkg.in/yaml.v2"
	"os"
	"strconv"
	"strings"
)

func LoadConfigFromFile(path string) (MasterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return MasterConfig{}, err
	}

	var cfg MasterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return MasterConfig{}, err
	}

	return cfg, nil
}

func LoadConfigFromFlags() ServerConfig {
	cfg := ServerConfig{}

	flag.StringVar(&cfg.Name, "name", "", "Server name")
	flag.StringVar(&cfg.Address, "addr", "", "Server address")
	flag.StringVar(&cfg.P2PAddress, "p2p-addr", "", "P2P address")
	flag.StringVar(&cfg.Service, "service", "", "Service name (for workers)")
	flag.StringVar(&cfg.Protocol, "protocol", "text", "Protocol type")

	// 解析引导节点
	var bootstrap string
	flag.StringVar(&bootstrap, "bootstrap", "", "Comma-separated bootstrap peers")

	// 自定义参数存储
	extra := map[string]interface{}{}
	flag.Var(&extraFlags{values: extra}, "set", "Set extra config values (key=value)")

	flag.Parse()

	// 处理引导节点
	if bootstrap != "" {
		cfg.Bootstrap = strings.Split(bootstrap, ",")
	}

	return cfg
}

// 自定义flag类型用于额外参数
type extraFlags struct {
	values map[string]interface{}
}

func (f *extraFlags) String() string {
	return ""
}

func (f *extraFlags) Set(value string) error {
	parts := strings.SplitN(value, "=", 2)
	if len(parts) != 2 {
		return errors.New("expected key=value format")
	}

	key := parts[0]
	val := parts[1]

	// 尝试解析为数字
	if intVal, err := strconv.Atoi(val); err == nil {
		f.values[key] = intVal
	} else if boolVal, err := strconv.ParseBool(val); err == nil {
		f.values[key] = boolVal
	} else {
		f.values[key] = val
	}

	return nil
}
