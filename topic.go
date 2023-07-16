package simplequeue

import (
	"log"
	"sync"

	"github.com/forfd8960/simplequeue/pb"
)

const (
	defaultMemQueueSize = 100
)

type Topic struct {
	mu            sync.RWMutex
	qs            *QueueServer
	Name          string
	ChannelMap    map[string]*Channel
	MemoryMsgChan chan *pb.QueueMsg

	wg waitGroup
}

func NewTopic(topicName string, qs *QueueServer) *Topic {
	t := &Topic{
		qs:            qs,
		Name:          topicName,
		ChannelMap:    make(map[string]*Channel),
		MemoryMsgChan: make(chan *pb.QueueMsg, defaultMemQueueSize), //todo: read mem queue size from qs
	}

	t.wg.Wrap(t.messagePump)
	return t
}

func (t *Topic) messagePump() {
	var msg *pb.QueueMsg
	var chans []*Channel

	t.mu.RLock()
	for _, ch := range t.ChannelMap {
		chans = append(chans, ch)
	}
	t.mu.RUnlock()

	for {
		select {
		case msg = <-t.MemoryMsgChan:
		default:
			log.Println("no msg in channel")
			continue
		}

		for i, ch := range chans {
			chanMsg := msg
			if i > 0 {
				chanMsg = NewMessage(msg.Id, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
			}

			if err := ch.PutMessage(chanMsg); err != nil {
				log.Printf("put message to channel: %s, err: %v", ch.Name, err)
			}
		}
	}
}
