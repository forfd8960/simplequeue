package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/forfd8960/simplequeue/pb"
)

func main() {
	queueServerAddrs := flag.String("queue-addrs", "localhost:8090", "the queue server address")
	flag.Parse()

	log.Println("Pub message to queue server: ", *queueServerAddrs)

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

	resp, err := client.PubMessage(ctx, &pb.PubMessageRequest{
		Pub: &pb.Pub{
			Topic: "hello-topic",
			Msg:   []byte(`Hello Hello Hello - How are you`),
		},
	})
	if err != nil {
		log.Printf("[Producer] PubMessage error: %v\n", err)
		os.Exit(1)
	}

	log.Printf("[Producer] PubMessage: %v\n", resp)

}
