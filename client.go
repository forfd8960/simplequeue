package simplequeue

type Client struct {
	// qs *QueueServer

	ID       int64
	Hostname string
	Channel  *Channel
	// SubEventChan chan *Channel
}

// NewClient ...
func NewClient(id int64) *Client {
	return &Client{
		ID: id,
		// qs: qs,
		// SubEventChan: make(chan *Channel, 100),
	}
}
