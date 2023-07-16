package simplequeue

// IOLoop is forward message from producer to consumer
/*

1. Producer Pub Message to a Topic through QueueServer: RPC PubMessage

2. Topic put the message to Topic's MemoryMsgChan

3. Topic copy the msg from MemoryMsgChan to Channel's MemoryMsgChan(this for loop is started when newTopic)

4. QueueServer get msg from client.SubEventChan.MemoryMsgChan, and send it to client through grpc stream Send.
*/
func (qs *QueueServer) IOLoop() error {
	return nil
}

func (qs *QueueServer) messagePump() {

}
