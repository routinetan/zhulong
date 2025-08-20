package common

import (
	"bytes"
	"net"
)

const (
	CmdSendToClient = 1 // 发送数据给客户端
	CmdBindUid      = 2 // 绑定UID
	CmdUnbindUid    = 3 // 解绑UID
	CmdJoinGroup    = 4 // 加入分组
	CmdSendToGroup  = 5
	CmdNewMessage   = 6
)

type Protocol interface {
	Input(buffer []byte) (int, error)
	Decode(buffer []byte) (interface{}, error)
	Encode(data interface{}) ([]byte, error)
}

// 在连接处理器中使用协议
type ProtocolHandler struct {
	Protocol Protocol
	Handler  func(data interface{}, conn net.Conn)
}

func (h *ProtocolHandler) HandleConn(conn net.Conn) {
	buffer := make([]byte, 4096)
	readBuf := bytes.NewBuffer(nil)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			// 处理错误
			return
		}

		readBuf.Write(buffer[:n])

		for {
			packetLen, err := h.Protocol.Input(readBuf.Bytes())
			if err != nil {
				// 处理协议错误
				return
			}

			if packetLen == 0 || readBuf.Len() < packetLen {
				break
			}

			packet := readBuf.Next(packetLen)
			data, err := h.Protocol.Decode(packet)
			if err != nil {
				// 处理解码错误
				continue
			}

			h.Handler(data, conn)
		}
	}
}
