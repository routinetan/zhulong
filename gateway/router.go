package gateway

import (
	"zhulong/register"
)

type WorkerRouter struct {
	Register *register.Registry
	ConnMgr  *ConnectionManager
}
