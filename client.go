package simplequeue

type Client struct {
	ID           int64
	qs           *QueueServer
	ClientID     string
	Hostname     string
	Channel      *Channel
	SubEventChan chan *Channel
}

// NewClient ...
func NewClient(id int64, qs *QueueServer) *Client {
	return &Client{
		ID:           id,
		qs:           qs,
		SubEventChan: make(chan *Channel, 1),
	}
}
