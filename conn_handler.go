package simplequeue

import "sync"

type connHandler struct {
	qs    *QueueServer
	conns sync.Map
}
