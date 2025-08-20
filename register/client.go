package register

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type RegistryClient struct {
	client   *redis.Client
	serverID string
}

func NewRegistryClient(addr string) *RegistryClient {
	return &RegistryClient{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: "", // 密码
			DB:       0,  // 使用默认DB
		}),
	}
}

func (rc *RegistryClient) RegisterGateway(id, ip string, port int) error {
	ctx := context.Background()
	key := fmt.Sprintf("gateways:%s", id)
	data := map[string]interface{}{
		"ip":          ip,
		"port":        port,
		"last_active": time.Now().Unix(),
	}

	if err := rc.client.HSet(ctx, key, data).Err(); err != nil {
		return err
	}

	// 添加到网关集合
	return rc.client.SAdd(ctx, "gateway_set", id).Err()
}

func (rc *RegistryClient) UnregisterGateway(id string) error {
	ctx := context.Background()

	// 从集合中移除
	if err := rc.client.SRem(ctx, "gateway_set", id).Err(); err != nil {
		return err
	}

	// 删除网关信息
	return rc.client.Del(ctx, fmt.Sprintf("gateways:%s", id)).Err()
}

func (rc *RegistryClient) SendHeartbeat(serviceName string) error {
	ctx := context.Background()
	key := fmt.Sprintf("services:%s:%s", serviceName, rc.serverID)
	return rc.client.HSet(ctx, key, "last_heartbeat", time.Now().Unix()).Err()
}
