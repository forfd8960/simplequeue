package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/forfd8960/simplequeue/pb"
)

func main() {
	queueServerAddrs := flag.String("queue-addrs", "localhost:8080", "the queue server address")
	topic := flag.String("topic", "hello-topic", "the topic name")
	channel := flag.String("channel", "msg-chan1", "the channel name")
	flag.Parse()

	log.Println("Sub Event to queue server: ", *queueServerAddrs)

	conn, err := grpc.Dial(*queueServerAddrs, grpc.WithTransportCredentials(
		insecure.NewCredentials(),
	))
	if err != nil {
		fmt.Println("grpc dail error: ", err)
		os.Exit(1)
	}
	defer conn.Close()

	ctx := context.Background()
	client := pb.NewQueueServiceClient(conn)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := client.ConsumeMessage(ctx, &pb.ConsumeMessageRequest{
		Sub: &pb.Sub{
			Topic:   *topic,
			Channel: *channel,
		},
	})
	if err != nil {
		log.Printf("[Consumer] ConsumeMessage err: %v\n", err)
		os.Exit(1)
	}

	log.Printf("[Consumer] Successfully sub to: %s\n", *topic)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Printf("stream.Recv err: %v\n", err)
			continue
		}

		log.Println("received msg: ", *msg)
	}
}
