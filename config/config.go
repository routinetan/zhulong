// config/config.go
package config

type ServerType string

const (
	ServerTypeRegister ServerType = "register"
	ServerTypeGateway  ServerType = "gateway"
	ServerTypeWorker   ServerType = "worker"
)

type ServerConfig struct {
	Enabled       bool       `yaml:"enabled" json:"enabled"`
	Type          ServerType `yaml:"type" json:"type"`
	Name          string     `yaml:"name" json:"name"`
	Address       string     `yaml:"address" json:"address"`         // 主服务地址
	P2PAddress    string     `yaml:"p2p_address" json:"p2p_address"` // P2P服务地址
	Bootstrap     []string   `yaml:"bootstrap" json:"bootstrap"`     // 初始节点
	Service       string     `yaml:"service" json:"service"`         // 服务名称（Worker专用）
	Protocol      string     `yaml:"protocol" json:"protocol"`       // 协议类型
	AsIndependent bool       `yaml:"independent" json:"independent"` // 是否作为独立进程运行
}

type MasterConfig struct {
	Servers []ServerConfig `yaml:"servers" json:"servers"`
}
