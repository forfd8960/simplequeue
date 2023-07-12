package simplequeue

import (
	"github.com/forfd8960/simplequeue/pb"
)

type Consumer interface{}

type Channel struct {
	TopicName     string
	Name          string
	MemoryMsgChan chan *pb.QueueMsg

	qs      *QueueServer
	clients map[int64]Consumer
}
