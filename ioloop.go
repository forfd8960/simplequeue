package simplequeue

import "github.com/forfd8960/simplequeue/pb"

type sendMessageFN func(*pb.QueueMsg) error

// topicMessagePump is forward message from producer to consumer
/*

1. Producer Pub Message to a Topic through QueueServer: RPC PubMessage

2. Topic put the message to Topic's MemoryMsgChan

3. Topic copy the msg from MemoryMsgChan to Channel's MemoryMsgChan(this for loop is started when newTopic)

4. QueueServer get msg from client.SubEventChan.MemoryMsgChan, and send it to client through grpc stream Send.
*/
func (qs *QueueServer) topicMessagePump() {
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
	var subEventChan *Channel
	var msg *pb.QueueMsg
	for {
		select {
		case subEventChan = <-cli.SubEventChan:
		default:
			continue
		}

		select {
		case msg = <-subEventChan.MemoryMsgChan:
		default:
			continue
		}

		if msg != nil {
			if err := sendMsg(msg); err != nil {
				return err
			}
		}
	}
}
