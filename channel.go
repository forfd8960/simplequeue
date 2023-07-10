package simplequeue

import (
	"github.com/forfd8960/simplequeue/pb"
)

type Consumer interface{}

type Channel struct {
	TopicName     string
	Name          string
	MemoryMsgChan chan *pb.QueueMsg

	queueServer *QueueServer
	clients     map[int64]Consumer
}
