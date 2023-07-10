package simplequeue

import (
	"sync"

	"github.com/forfd8960/simplequeue/pb"
)

type Topic struct {
	mu            sync.Mutex
	Name          string
	ChannelMap    map[string]*Channel
	MemoryMsgChan chan *pb.QueueMsg

	queueServer *QueueServer
}
