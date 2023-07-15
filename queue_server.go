package simplequeue

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

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

type QueueServer struct {
	mu          sync.RWMutex
	topicMap    map[string]*Topic
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

func (qs *QueueServer) SubQueue(req *pb.SubQueueRequest, srv pb.QueueService_SubQueueServer) error {
	if req == nil || req.Sub == nil {
		return errInvalidArgument("invalid sub request: %v", req)
	}

	topic := req.Sub.Topic
	if topic == "" {
		return errInvalidArgument("empty topic")
	}

	channel := req.Sub.Channel
	if channel == "" {
		return errInvalidArgument("empty channel")
	}

	srv.Send(&pb.QueueMsg{
		Id:        "1",
		Body:      "Hello",
		Timestamp: time.Now().Unix(),
	})
	return nil
}
