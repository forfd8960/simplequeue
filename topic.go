package simplequeue

import (
	"log"
	"sync"
	"time"

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
	exitChan      chan struct{}
}

func NewTopic(topicName string, qs *QueueServer) *Topic {
	t := &Topic{
		qs:            qs,
		Name:          topicName,
		exitChan:      make(chan struct{}, 1),
		ChannelMap:    make(map[string]*Channel),
		MemoryMsgChan: make(chan *pb.QueueMsg, defaultMemQueueSize), //todo: read mem queue size from qs
	}

	return t
}

func (t *Topic) GetChannel(chanName string) *Channel {
	t.mu.Lock()
	defer t.mu.Unlock()
	channel, isNew := t.getOrCreateChannel(chanName)
	if isNew { //todo: update topic chan
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
		log.Printf("channel: %s exists\n", ch.Name)
		return ch, false
	}

	ch = NewChannel(t.Name, chanName, t.qs)
	t.ChannelMap[chanName] = ch
	log.Printf("create new channel: %s exists\n", ch.Name)
	return ch, true
}

func (t *Topic) messagePump() {
	var chans []*Channel

	t.mu.RLock()
	for _, ch := range t.ChannelMap {
		chans = append(chans, ch)
	}
	t.mu.RUnlock()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.sendMsgToChans(chans)
		case <-t.exitChan:
			return
		}
	}
}

func (t *Topic) sendMsgToChans(chans []*Channel) {
	var msg *pb.QueueMsg
	select {
	case msg = <-t.MemoryMsgChan:
	default:
		return
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
