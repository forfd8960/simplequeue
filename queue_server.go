package simplequeue

import (
	"net"
	"sync"
)

type QueueServer struct {
	mu          sync.RWMutex
	topicMap    map[string]*Topic
	tcpListener net.Listener
	connHandler *connHandler
	wg          *waitGroup
}

type Options struct {
	TCPAddress string `flag:"tcp-addr"`
}

// NewQueueServer ...
func NewQueueServer(opts *Options) (*QueueServer, error) {
	qs := &QueueServer{}
	qs.connHandler = &connHandler{
		qs: qs,
	}

	var err error
	qs.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, err
	}

	return qs, nil
}
