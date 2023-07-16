package simplequeue

import (
	"log"

	"github.com/forfd8960/simplequeue/pb"
)

type Consumer interface{}

type Channel struct {
	TopicName     string
	Name          string
	MemoryMsgChan chan *pb.QueueMsg

	qs      *QueueServer
	clients map[int64]*Client
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
