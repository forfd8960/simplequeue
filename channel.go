package simplequeue

import (
	"log"
	"sync"

	"github.com/forfd8960/simplequeue/pb"
)

type Consumer interface{}

type Channel struct {
	mu            sync.RWMutex
	TopicName     string
	Name          string
	MemoryMsgChan chan *pb.QueueMsg

	qs      *QueueServer
	clients map[int64]*Client
}

func NewChannel(topicName, chName string, qs *QueueServer) *Channel {
	return &Channel{
		qs:            qs,
		TopicName:     topicName,
		Name:          chName,
		MemoryMsgChan: make(chan *pb.QueueMsg, 1),
	}
}

//	AddClient ...
//
// todo: refactor cli to be interface
func (ch *Channel) AddClient(cliID int64, cli *Client) error {
	ch.mu.RLock()
	_, ok := ch.clients[cliID]
	ch.mu.RUnlock()
	if ok {
		return nil
	}

	ch.mu.Lock()
	ch.clients[cliID] = cli
	ch.mu.Unlock()
	return nil
}

func (ch *Channel) PutMessage(msg *pb.QueueMsg) error {
	select {
	case ch.MemoryMsgChan <- msg:
	default:
		log.Println("memory chan is full, write to backend")
		//todo: write msg to backend
	}

	return nil
}
