package simplequeue

import (
	"net"
	"sync"
)

type TcpHandler interface {
	Handle(conn net.Conn)
}

type connHandler struct {
	qs    *QueueServer
	conns sync.Map
}

func runServer(listner net.Listener, tcpHandler TcpHandler) error {
	return nil
}

func (connHdr *connHandler) Handle(conn net.Conn) {
	return
}
