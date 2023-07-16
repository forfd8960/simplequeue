package simplequeue

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"

	"github.com/forfd8960/simplequeue/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	mu          sync.RWMutex
	clientIDSeq int64
	topicMap    map[string]*Topic
	clients     map[ClientID]*Client
	tcpListener net.Listener
	connHandler *connHandler
	wg          *waitGroup
}

type Options struct {
	TCPAddress string `flag:"tcp-addr"`
}

// NewQueueServer ...
func NewQueueServer(opts *Options) (*QueueServer, error) {
	qs := &QueueServer{}
	qs.connHandler = &connHandler{
		qs: qs,
	}

	var err error
	qs.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, err
	}

	return qs, nil
}

func (qs *QueueServer) Main() error {
	exitCh := make(chan error)
	var once sync.Once

	exitFunc := func(err error) {
		once.Do(func() {
			exitCh <- err
		})
	}

	qs.wg.Wrap(func() {
		exitFunc(runServer(qs.tcpListener, qs.connHandler))
	})

	err := <-exitCh
	return err
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
	client := NewClient(clientID, qs)
	client.Channel = ch
	client.SubEventChan <- ch

	if err := ch.AddClient(clientID, client); err != nil {
		return nil, err
	}

	qs.mu.Lock()
	qs.clients[clientID] = client
	qs.mu.Unlock()

	return &pb.SubEventResponse{
		ClientId: fmt.Sprintf("%d", clientID),
	}, nil
}

func (qs *QueueServer) ConsumeMessage(req *pb.ConsumeMessageRequest, srv pb.QueueService_ConsumeMessageServer) error {
	if req == nil || req.ClientId == "" {
		return errInvalidArgument("invalid request/clientID: %v", req)
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
	qs.mu.Unlock()

	return t
}
