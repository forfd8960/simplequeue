package simplequeue

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	"github.com/rs/xid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/forfd8960/simplequeue/pb"
)

const (
	defaultTopicSize   = 100
	defaultClientCount = 100
)

var (
	errInvalidPubRequest = func(pubReq *pb.PubMessageRequest) error {
		return status.Errorf(codes.InvalidArgument, "Invalid Pub Message Request: %v", pubReq)
	}
	errInvalidArgument = func(msg string, args ...any) error {
		return status.Errorf(codes.InvalidArgument, msg, args)
	}
)

type ClientID = int64

type QueueServer struct {
	clientIDSeq int64

	mu         sync.RWMutex
	topicMap   map[string]*Topic
	topicsChan chan *Topic
	exitChan   chan struct{}
	wg         waitGroup
}

type Options struct {
	TopicChanSize int64 `flag:"topic-chan-size"`
	ClientCount   int64 `flag:"client-count"`
}

// NewQueueServer ...
func NewQueueServer(opts *Options) (*QueueServer, error) {
	topicChanSize := opts.TopicChanSize
	if topicChanSize < defaultTopicSize {
		topicChanSize = defaultTopicSize
	}

	clientCount := opts.ClientCount
	if clientCount < defaultClientCount {
		clientCount = defaultClientCount
	}

	qs := &QueueServer{
		topicMap:   make(map[string]*Topic, topicChanSize),
		topicsChan: make(chan *Topic, topicChanSize),
		exitChan:   make(chan struct{}, 1),
	}
	qs.wg.Wrap(qs.topicMessagePump)
	return qs, nil
}

func (qs *QueueServer) PubMessage(ctx context.Context, req *pb.PubMessageRequest) (*pb.PubMessageResponse, error) {
	if req == nil || req.Pub == nil {
		return nil, errInvalidPubRequest(req)
	}
	if req.Pub.Topic == "" {
		return nil, errInvalidArgument("empty topic")
	}

	if len(req.Pub.Msg) == 0 {
		return nil, errInvalidArgument("empty message")
	}
	log.Printf("[QueueServer] Incoming req: %+v\n", req)

	topic := qs.GetTopic(req.Pub.Topic)
	log.Printf("[QueueServer] get topic: %s\n", topic.Name)

	msgID := xid.New().String()
	msg := NewMessage(msgID, string(req.Pub.Msg))

	log.Printf("[QueueServer] new message: %+v\n", msg)
	if err := topic.PutMessage(msg); err != nil {
		return nil, err
	}

	log.Printf("[QueueServer] Success Put Msg: %+v, to: %s\n", msg, topic.Name)
	return &pb.PubMessageResponse{}, nil
}

func (qs *QueueServer) ConsumeMessage(req *pb.ConsumeMessageRequest, srv pb.QueueService_ConsumeMessageServer) error {
	if req == nil || req.Sub == nil {
		return errInvalidArgument("invalid request: %v", req)
	}

	topic := req.Sub.Topic
	if topic == "" {
		return errInvalidArgument("empty topic")
	}

	channel := req.Sub.Channel
	if channel == "" {
		return errInvalidArgument("empty channel")
	}

	t := qs.GetTopic(topic)
	log.Printf("Sub to Topic: %s\n", t.Name)

	ch := t.GetChannel(channel)
	log.Printf("Sub to Channel: %s\n", ch.Name)

	clientID := atomic.AddInt64(&qs.clientIDSeq, 1)
	client := NewClient(clientID)
	client.Channel = ch

	if err := qs.messagePump(client, srv.Send); err != nil {
		return err
	}

	return nil
}

func (qs *QueueServer) GetTopic(topicName string) *Topic {
	qs.mu.RLock()
	t, ok := qs.topicMap[topicName]
	if ok {
		return t
	}
	qs.mu.RUnlock()

	qs.mu.Lock()
	t, ok = qs.topicMap[topicName]
	if ok {
		return t
	}

	t = NewTopic(topicName, qs)
	qs.topicMap[topicName] = t

	select {
	case qs.topicsChan <- t:
	default:
		log.Println("topicChan is full")
	}
	qs.mu.Unlock()

	return t
}
