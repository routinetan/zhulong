// register/service.go
package register

import (
	"sync"
	"time"

	"math/rand"
)

type ServiceNode struct {
	IP         string
	Port       int
	LastActive int64 // 最后活跃时间戳
}

type Registry struct {
	sync.RWMutex
	Gateways map[string]*ServiceNode // key: gateway_id
	Workers  map[string]*ServiceNode // key: worker_id
}

func (r *Registry) RegisterGateway(id, ip string, port int) {
	r.Lock()
	defer r.Unlock()
	r.Gateways[id] = &ServiceNode{IP: ip, Port: port, LastActive: time.Now().Unix()}
}

func (r *Registry) GetGateways() []*ServiceNode {
	r.RLock()
	defer r.RUnlock()
	nodes := make([]*ServiceNode, 0, len(r.Gateways))
	for _, node := range r.Gateways {
		nodes = append(nodes, node)
	}
	return nodes
}

// 心跳检测协程
func (r *Registry) StartHeartbeatCheck() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			<-ticker.C
			//r.removeExpiredNodes(30) // 清理30秒未心跳的节点
		}
	}()
}

func (r *Registry) SelectWorker() *ServiceNode {
	r.RLock()
	defer r.RUnlock()

	// 随机选择策略
	keys := make([]string, 0, len(r.Workers))
	for k := range r.Workers {
		keys = append(keys, k)
	}
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	return r.Workers[keys[0]]
}
