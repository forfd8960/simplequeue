package simplequeue

import (
	"fmt"
	"log"

	"github.com/forfd8960/simplequeue/pb"
)

type sendMessageFN func(*pb.QueueMsg) error

// topicMessagePump is forward message from producer to consumer
/*

1. Producer Pub Message to a Topic through QueueServer: RPC PubMessage

2. Topic put the message to Topic's MemoryMsgChan

3. Topic copy the msg from MemoryMsgChan to Channel's MemoryMsgChan(this for loop is started when newTopic)

4. QueueServer get msg from client.SubEventChan.MemoryMsgChan, and send it to client through grpc stream Send.
*/
func (qs *QueueServer) topicMessagePump() {
	log.Println("--------Starting Topic Message Pump---------")
	var topic *Topic
	for {
		select {
		case topic = <-qs.topicsChan:
		default:
			continue
		}

		qs.wg.Wrap(topic.messagePump)
	}
}

func (qs *QueueServer) messagePump(cli *Client, sendMsg sendMessageFN) error {
	println("--------Starting Client Message Pump--------")

	log.Printf("cli channel: %v\n", cli.SubEventChan)
	log.Printf("cli channel msg length: %d\n", len(cli.Channel.MemoryMsgChan))

	// var subEventChan *Channel
	var msg *pb.QueueMsg

	channel := cli.Channel
	if channel == nil {
		return fmt.Errorf("empty channel")
	}

	for {
		// select {
		// case subEventChan = <-cli.SubEventChan:
		// default:
		// 	continue
		// }

		select {
		case msg = <-channel.MemoryMsgChan:
		default:
			continue
		}

		if msg != nil {
			log.Println("Send Msg: ", msg)
			if err := sendMsg(msg); err != nil {
				return err
			}
		}
	}
}
