package simplequeue

import (
	"time"

	"github.com/forfd8960/simplequeue/pb"
)

func NewMessage(id string, body string) *pb.QueueMsg {
	return &pb.QueueMsg{
		Id:        id,
		Body:      body,
		Timestamp: time.Now().Unix(),
	}
}
