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
}

func NewTopic(topicName string, qs *QueueServer) *Topic {
	t := &Topic{
		qs:            qs,
		Name:          topicName,
		ChannelMap:    make(map[string]*Channel),
		MemoryMsgChan: make(chan *pb.QueueMsg, defaultMemQueueSize), //todo: read mem queue size from qs
	}

	return t
}

func (t *Topic) GetChannel(chanName string) *Channel {
	t.mu.RLock()
	channel, isNew := t.getOrCreateChannel(chanName)
	t.mu.RUnlock()

	//todo: update topic chan
	if isNew {
		log.Println("created new channel: ", chanName)
	}
	return channel
}

func (t *Topic) PutMessage(msg *pb.QueueMsg) error {
	select {
	case t.MemoryMsgChan <- msg:
		return nil
	default: // todo: write msg to filestorage
		log.Println("memoryMsgChan is full")
	}

	return nil
}

func (t *Topic) getOrCreateChannel(chanName string) (*Channel, bool) {
	ch, ok := t.ChannelMap[chanName]
	if ok {
		return ch, false
	}

	ch = NewChannel(t.Name, chanName, t.qs)
	t.ChannelMap[chanName] = ch
	return ch, true
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
