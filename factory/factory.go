// factory/factory.go
package factory

import (
	"errors"
	"zhulong/common"
	"zhulong/config"
	"zhulong/gateway"
)

type ServerInstance struct {
	Config config.ServerConfig
	Server common.ServerRunnable
}

func CreateServer(cfg config.ServerConfig) (*ServerInstance, error) {
	switch cfg.Type {
	case config.ServerTypeGateway:
		return createGatewayServer(cfg)
	case config.ServerTypeRegister:
		return createRegisterServer(cfg)
	default:
		return nil, errors.New("unknown server type")
	}
}

func createGatewayServer(cfg config.ServerConfig) (*ServerInstance, error) {

	server, err := gateway.NewGatewayServer(
		cfg.Address,
		&gateway.TextProtocol{},
	)
	if err != nil {
		return nil, err
	}

	return &ServerInstance{
		Config: cfg,
		Server: server,
	}, nil
}

func createRegisterServer(cfg config.ServerConfig) (*ServerInstance, error) {
	server := gateway.NewRegisterServer(
		cfg.Address,
	)
	return &ServerInstance{
		Config: cfg,
		Server: server,
	}, nil
}
