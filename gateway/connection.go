// gateway/connection.go
package gateway

import (
	"errors"
	"net"
	"sync"
	"time"
)

type Client struct {
	Conn        net.Conn
	ID          string
	UID         string
	Groups      map[string]bool
	ConnectedAt time.Time
	LastActive  time.Time
}

type ConnectionManager struct {
	sync.RWMutex
	Clients     map[string]*Client         // connID => Client
	UIDToConn   map[string]map[string]bool // uid => [connID]
	GroupToConn map[string]map[string]bool // group => [connID]
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		Clients:     make(map[string]*Client),
		UIDToConn:   make(map[string]map[string]bool),
		GroupToConn: make(map[string]map[string]bool),
	}
}

// AddClient 添加新客户端连接
func (cm *ConnectionManager) AddClient(conn net.Conn) *Client {
	connID := generateConnID(conn)

	client := &Client{
		Conn:        conn,
		ID:          connID,
		Groups:      make(map[string]bool),
		ConnectedAt: time.Now(),
		LastActive:  time.Now(),
	}

	cm.Lock()
	defer cm.Unlock()

	cm.Clients[connID] = client
	return client
}

// RemoveClient 移除客户端连接
func (cm *ConnectionManager) RemoveClient(connID string) {
	cm.Lock()
	defer cm.Unlock()

	if client, ok := cm.Clients[connID]; ok {
		// 从UID映射中移除
		if client.UID != "" {
			if connMap, ok := cm.UIDToConn[client.UID]; ok {
				delete(connMap, connID)
				if len(connMap) == 0 {
					delete(cm.UIDToConn, client.UID)
				}
			}
		}

		// 从群组映射中移除
		for group := range client.Groups {
			if connMap, ok := cm.GroupToConn[group]; ok {
				delete(connMap, connID)
				if len(connMap) == 0 {
					delete(cm.GroupToConn, group)
				}
			}
		}

		// 从主连接表中移除
		delete(cm.Clients, connID)
	}
}

// GetClient 获取客户端
func (cm *ConnectionManager) GetClient(connID string) (*Client, bool) {
	cm.RLock()
	defer cm.RUnlock()

	client, ok := cm.Clients[connID]
	return client, ok
}

// UpdateActivity 更新客户端活跃时间
func (cm *ConnectionManager) UpdateActivity(connID string) {
	cm.Lock()
	defer cm.Unlock()

	if client, ok := cm.Clients[connID]; ok {
		client.LastActive = time.Now()
	}
}

// BindUID 绑定UID
func (cm *ConnectionManager) BindUID(connID, uid string) {
	cm.Lock()
	defer cm.Unlock()

	if client, ok := cm.Clients[connID]; ok {
		// 解除旧的UID绑定
		if client.UID != "" && client.UID != uid {
			if connMap, ok := cm.UIDToConn[client.UID]; ok {
				delete(connMap, connID)
				if len(connMap) == 0 {
					delete(cm.UIDToConn, client.UID)
				}
			}
		}

		// 设置新UID
		client.UID = uid

		// 添加到UID映射
		if cm.UIDToConn[uid] == nil {
			cm.UIDToConn[uid] = make(map[string]bool)
		}
		cm.UIDToConn[uid][connID] = true
	}
}

// JoinGroup 加入群组
func (cm *ConnectionManager) JoinGroup(connID, group string) {
	cm.Lock()
	defer cm.Unlock()

	if client, ok := cm.Clients[connID]; ok {
		client.Groups[group] = true

		// 添加到群组映射
		if cm.GroupToConn[group] == nil {
			cm.GroupToConn[group] = make(map[string]bool)
		}
		cm.GroupToConn[group][connID] = true
	}
}

// LeaveGroup 离开群组
func (cm *ConnectionManager) LeaveGroup(connID, group string) {
	cm.Lock()
	defer cm.Unlock()

	if client, ok := cm.Clients[connID]; ok {
		delete(client.Groups, group)

		// 从群组映射中移除
		if connMap, ok := cm.GroupToConn[group]; ok {
			delete(connMap, connID)
			if len(connMap) == 0 {
				delete(cm.GroupToConn, group)
			}
		}
	}
}

// SendToClient 发送消息给指定客户端
func (cm *ConnectionManager) SendToClient(connID string, data []byte) error {
	cm.RLock()
	defer cm.RUnlock()

	if client, ok := cm.Clients[connID]; ok {
		_, err := client.Conn.Write(data)
		cm.UpdateActivity(connID) // 更新活跃时间
		return err
	}
	return errors.New("connection not found")
}

// SendToUID 发送消息给指定UID的所有连接
func (cm *ConnectionManager) SendToUID(uid string, data []byte) []error {
	cm.RLock()
	defer cm.RUnlock()

	var errors []error
	if connMap, ok := cm.UIDToConn[uid]; ok {
		for connID := range connMap {
			if client, ok := cm.Clients[connID]; ok {
				if _, err := client.Conn.Write(data); err != nil {
					errors = append(errors, err)
				} else {
					cm.UpdateActivity(connID)
				}
			}
		}
	}
	return errors
}

// SendToGroup 发送消息给群组
func (cm *ConnectionManager) SendToGroup(group string, data []byte) []error {
	cm.RLock()
	defer cm.RUnlock()

	var errors []error
	if connMap, ok := cm.GroupToConn[group]; ok {
		for connID := range connMap {
			if client, ok := cm.Clients[connID]; ok {
				if _, err := client.Conn.Write(data); err != nil {
					errors = append(errors, err)
				} else {
					cm.UpdateActivity(connID)
				}
			}
		}
	}
	return errors
}

// CloseAll 关闭所有连接
func (cm *ConnectionManager) CloseAll() {
	cm.Lock()
	defer cm.Unlock()

	for _, client := range cm.Clients {
		client.Conn.Close()
	}

	// 清空所有映射
	cm.Clients = make(map[string]*Client)
	cm.UIDToConn = make(map[string]map[string]bool)
	cm.GroupToConn = make(map[string]map[string]bool)
}

// 生成连接ID (示例实现)
func generateConnID(conn net.Conn) string {
	addr := conn.RemoteAddr().String()
	return addr + "-" + time.Now().Format("20060102150405.999999")
}
