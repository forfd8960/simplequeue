package simplequeue

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

const (
	erruUseOfClosedNetConn = "use of closed network connection"
)

type TcpHandler interface {
	Handle(conn net.Conn)
}

type connHandler struct {
	qs    *QueueServer
	conns sync.Map
}

func runServer(listner net.Listener, tcpHandler TcpHandler) error {
	var wg sync.WaitGroup
	for {
		clientConn, err := listner.Accept()
		if err != nil {
			if !strings.Contains(err.Error(), erruUseOfClosedNetConn) {
				return fmt.Errorf("listener.Accept() error: %v", err)
			}

			break
		}

		wg.Add(1)
		go func() {
			tcpHandler.Handle(clientConn)
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}

func (connHdr *connHandler) Handle(conn net.Conn) {
	return
}
