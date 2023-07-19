package main

import (
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/forfd8960/simplequeue"
	"github.com/forfd8960/simplequeue/pb"
)

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Printf("listen error: %v\n", err)
		return
	}

	s := grpc.NewServer()

	queueServer, err := simplequeue.NewQueueServer(&simplequeue.Options{
		TopicChanSize: 100,
		ClientCount:   1000,
	})
	if err != nil {
		log.Printf("Init server err: %v\n", err)
		os.Exit(1)
	}

	pb.RegisterQueueServiceServer(s, queueServer)

	log.Println("-----------Start queueServer On 8080---------")
	if err = s.Serve(lis); err != nil {
		log.Printf("serve err: %v\n", err)
		os.Exit(1)
	}
}
