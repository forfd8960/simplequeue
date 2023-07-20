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
	clients    map[ClientID]*Client
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
		clients:    make(map[int64]*Client, clientCount),
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

func (qs *QueueServer) SubEvent(ctx context.Context, req *pb.SubEventRequest) (*pb.SubEventResponse, error) {
	if req == nil || req.Sub == nil {
		return nil, errInvalidArgument("invalid sub request: %v", req)
	}

	topic := req.Sub.Topic
	if topic == "" {
		return nil, errInvalidArgument("empty topic")
	}

	channel := req.Sub.Channel
	if channel == "" {
		return nil, errInvalidArgument("empty channel")
	}

	t := qs.GetTopic(topic)
	ch := t.GetChannel(channel)

	clientID := atomic.AddInt64(&qs.clientIDSeq, 1)
	client := NewClient(clientID)
	client.Channel = ch

	if err := ch.AddClient(clientID, client); err != nil {
		return nil, err
	}

	log.Println("adding client to queue server: ", client.ID)
	qs.addClient(client)
	log.Println("done add client to queue server: ", client.ID)

	return &pb.SubEventResponse{
		ClientId: clientID,
	}, nil
}

func (qs *QueueServer) addClient(cli *Client) {
	qs.mu.Lock()
	qs.clients[cli.ID] = cli
	qs.mu.Unlock()
}

func (qs *QueueServer) ConsumeMessage(req *pb.ConsumeMessageRequest, srv pb.QueueService_ConsumeMessageServer) error {
	if req == nil || req.ClientId == 0 {
		return errInvalidArgument("invalid request/clientID: %v", req)
	}

	cli, err := qs.getClient(req.ClientId)
	if err != nil {
		return err
	}

	if err := qs.messagePump(cli, srv.Send); err != nil {
		return err
	}

	return nil
}

func (qs *QueueServer) getClient(cliID int64) (*Client, error) {
	qs.mu.RLock()
	defer qs.mu.RUnlock()
	cli, ok := qs.clients[cliID]
	if !ok || cli == nil {
		return nil, errInvalidArgument("client not found, clientID: %d", cliID)
	}

	return cli, nil
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
	qs.mu.Unlock()

	select {
	case qs.topicsChan <- t:
	default:
		log.Println("topicChan is full")
	}

	return t
}
