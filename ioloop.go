package simplequeue

import (
	"fmt"
	"log"
	"time"

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

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			qs.startTopicMessage()
		case <-qs.exitChan:
			return
		}
	}
}

func (qs *QueueServer) startTopicMessage() {
	select {
	case t := <-qs.topicsChan:
		qs.wg.Wrap(t.messagePump)
	default:
		return
	}
}

func (qs *QueueServer) messagePump(cli *Client, sendMsg sendMessageFN) error {
	println("--------Starting Client Message Pump--------")

	log.Printf("cli channel: %v\n", cli.Channel.Name)
	log.Printf("cli channel msg length: %d\n", len(cli.Channel.MemoryMsgChan))

	// var subEventChan *Channel

	channel := cli.Channel
	if channel == nil {
		return fmt.Errorf("empty channel")
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sendMessage(channel, sendMsg); err != nil {
				return err
			}
		case <-qs.exitChan:
			return nil
		}
	}
}

func sendMessage(channel *Channel, sendMsg sendMessageFN) error {
	var msg *pb.QueueMsg
	select {
	case msg = <-channel.MemoryMsgChan:
	default:
		return nil
	}

	if msg == nil {
		return nil
	}

	log.Println("Send Msg: ", msg)
	if err := sendMsg(msg); err != nil {
		return err
	}

	return nil
}
