// gateway/protocol.go
package gateway

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"zhulong/common"
)

// TextProtocol 文本协议实现
type TextProtocol struct{}

func NewTextProtocol() *TextProtocol {
	return &TextProtocol{}
}

func (p *TextProtocol) Input(buffer []byte) (int, error) {
	// 查找换行符作为消息结束符
	if idx := bytes.IndexByte(buffer, '\n'); idx != -1 {
		return idx + 1, nil
	}
	return 0, nil
}

func (p *TextProtocol) Decode(buffer []byte) (interface{}, error) {
	// 去除换行符
	return bytes.TrimSpace(buffer), nil
}

func (p *TextProtocol) Encode(data interface{}) ([]byte, error) {
	if str, ok := data.(string); ok {
		return []byte(str + "\n"), nil
	}
	if bytes, ok := data.([]byte); ok {
		return append(bytes, '\n'), nil
	}
	return nil, errors.New("unsupported data type")
}

// BinaryProtocol 二进制协议实现
type BinaryProtocol struct{}

func (p *BinaryProtocol) Input(buffer []byte) (int, error) {
	if len(buffer) < 4 {
		return 0, nil
	}
	// 头部4字节表示消息长度
	length := binary.BigEndian.Uint32(buffer[:4])
	if len(buffer) >= int(length)+4 {
		return int(length) + 4, nil
	}
	return 0, nil
}

func (p *BinaryProtocol) Decode(buffer []byte) (interface{}, error) {
	if len(buffer) < 4 {
		return nil, errors.New("invalid binary packet")
	}
	// 跳过4字节长度头
	return buffer[4:], nil
}

func (p *BinaryProtocol) Encode(data interface{}) ([]byte, error) {
	var body []byte
	switch v := data.(type) {
	case string:
		body = []byte(v)
	case []byte:
		body = v
	default:
		return nil, errors.New("unsupported data type")
	}

	// 创建带长度头的缓冲区
	buf := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(body)))
	copy(buf[4:], body)
	return buf, nil
}

// WebSocketProtocol WebSocket协议实现
type WebSocketProtocol struct {
	// 可添加WebSocket特定配置
}

func (p *WebSocketProtocol) Input(buffer []byte) (int, error) {
	// 简化的WebSocket帧检测
	if len(buffer) < 2 {
		return 0, nil
	}

	// 解析WebSocket帧头
	//fin := (buffer[0] & 0x80) != 0
	//opcode := buffer[0] & 0x0F
	payloadLen := uint64(buffer[1] & 0x7F)

	headerSize := 2
	if payloadLen == 126 {
		if len(buffer) < 4 {
			return 0, nil
		}
		payloadLen = uint64(binary.BigEndian.Uint16(buffer[2:4]))
		headerSize = 4
	} else if payloadLen == 127 {
		if len(buffer) < 10 {
			return 0, nil
		}
		payloadLen = binary.BigEndian.Uint64(buffer[2:10])
		headerSize = 10
	}

	// 检查是否有掩码
	mask := (buffer[1] & 0x80) != 0
	if mask {
		headerSize += 4
	}

	// 检查数据是否完整
	totalLen := headerSize + int(payloadLen)
	if len(buffer) < totalLen {
		return 0, nil
	}

	return totalLen, nil
}

func (p *WebSocketProtocol) Decode(buffer []byte) (interface{}, error) {
	// 简化解码实现
	//fin := (buffer[0] & 0x80) != 0
	opcode := buffer[0] & 0x0F
	payloadLen := uint64(buffer[1] & 0x7F)

	headerSize := 2
	if payloadLen == 126 {
		payloadLen = uint64(binary.BigEndian.Uint16(buffer[2:4]))
		headerSize = 4
	} else if payloadLen == 127 {
		payloadLen = binary.BigEndian.Uint64(buffer[2:10])
		headerSize = 10
	}

	mask := (buffer[1] & 0x80) != 0
	maskKey := []byte{}
	if mask {
		maskKey = buffer[headerSize : headerSize+4]
		headerSize += 4
	}

	payload := buffer[headerSize : headerSize+int(payloadLen)]

	// 应用掩码
	if mask {
		for i := 0; i < len(payload); i++ {
			payload[i] ^= maskKey[i%4]
		}
	}

	// 只处理文本和二进制帧
	if opcode == 0x1 || opcode == 0x2 {
		return payload, nil
	}

	return nil, errors.New("unsupported WebSocket opcode")
}

func (p *WebSocketProtocol) Encode(data interface{}) ([]byte, error) {
	var payload []byte
	switch v := data.(type) {
	case string:
		payload = []byte(v)
	case []byte:
		payload = v
	default:
		return nil, errors.New("unsupported data type")
	}

	// 创建WebSocket帧
	frame := make([]byte, 10) // 最大头长度
	frame[0] = 0x82           // FIN + 二进制帧

	// 设置长度
	payloadLen := len(payload)
	if payloadLen <= 125 {
		frame[1] = byte(payloadLen)
		frame = frame[:2]
	} else if payloadLen <= 65535 {
		frame[1] = 126
		binary.BigEndian.PutUint16(frame[2:4], uint16(payloadLen))
		frame = frame[:4]
	} else {
		frame[1] = 127
		binary.BigEndian.PutUint64(frame[2:10], uint64(payloadLen))
		frame = frame[:10]
	}

	// 添加负载
	frame = append(frame, payload...)
	return frame, nil
}

// WorkerMessage 业务进程发往Gateway的消息结构
type WorkerMessage struct {
	Header WorkerHeader
	Body   []byte
	ConnID string // 连接标识
}

// WorkerHeader 消息头定义
type WorkerHeader struct {
	Cmd        uint8   // 命令类型
	LocalIP    [4]byte // 网关本地IP
	LocalPort  uint16  // 网关本地端口
	ClientIP   [4]byte // 客户端IP
	ClientPort uint16  // 客户端端口
	ExtDataLen uint16  // 扩展数据长度
}

// 解析业务进程发来的消息
func ParseWorkerMessage(r io.Reader) (*WorkerMessage, error) {
	// 1. 读取固定长度包头
	headerBuf := make([]byte, 13) // 1(cmd) + 4(ip) + 2(port) + 4(ip) + 2(port) = 13字节
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err
	}

	msg := &WorkerMessage{}
	buf := bytes.NewReader(headerBuf)

	// 2. 解析包头
	if err := binary.Read(buf, binary.BigEndian, &msg.Header.Cmd); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &msg.Header.LocalIP); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &msg.Header.LocalPort); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &msg.Header.ClientIP); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &msg.Header.ClientPort); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &msg.Header.ExtDataLen); err != nil {
		return nil, err
	}

	// 3. 读取扩展数据（包含连接ID）
	if msg.Header.ExtDataLen > 0 {
		extData := make([]byte, msg.Header.ExtDataLen)
		if _, err := io.ReadFull(r, extData); err != nil {
			return nil, err
		}
		// 扩展数据格式：前8字节为连接ID，其余为保留字段
		if len(extData) >= 8 {
			msg.ConnID = string(extData[:8])
		}
	}

	// 4. 读取消息体（根据协议规则判断是否有消息体）
	switch msg.Header.Cmd {
	case common.CmdSendToClient, common.CmdBindUid, common.CmdSendToGroup:
		// 读取消息体长度前缀
		var bodyLen uint32
		if err := binary.Read(r, binary.BigEndian, &bodyLen); err != nil {
			return nil, err
		}

		if bodyLen > 0 {
			msg.Body = make([]byte, bodyLen)
			if _, err := io.ReadFull(r, msg.Body); err != nil {
				return nil, err
			}
		}
	}

	return msg, nil
}

// 将解析后的消息编码回二进制
func EncodeWorkerMessage(msg *WorkerMessage) []byte {
	buf := new(bytes.Buffer)

	// 1. 写固定头部
	binary.Write(buf, binary.BigEndian, msg.Header.Cmd)
	binary.Write(buf, binary.BigEndian, msg.Header.LocalIP)
	binary.Write(buf, binary.BigEndian, msg.Header.LocalPort)
	binary.Write(buf, binary.BigEndian, msg.Header.ClientIP)
	binary.Write(buf, binary.BigEndian, msg.Header.ClientPort)

	// 2. 处理扩展数据
	extData := []byte(msg.ConnID)
	if len(extData) > 65535 {
		extData = extData[:65535]
	}
	extLen := uint16(len(extData))
	binary.Write(buf, binary.BigEndian, extLen)
	buf.Write(extData)

	// 3. 处理消息体
	if len(msg.Body) > 0 {
		bodyLen := uint32(len(msg.Body))
		binary.Write(buf, binary.BigEndian, bodyLen)
		buf.Write(msg.Body)
	}

	return buf.Bytes()
}

// IP转换工具函数
func ipToBytes(ipStr string) [4]byte {
	ip := net.ParseIP(ipStr).To4()
	var result [4]byte
	copy(result[:], ip)
	return result
}
