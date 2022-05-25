package p2p

import (
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

type MsgNotification struct {
	RemotePeer    peer.ID
	QueryDuration time.Duration
	Msg           pb.Message
	Resp          pb.Message
	Error         error
}

// Notifier will keep track of the
type Notifier struct {
	// so far with the pb.Messages we can already retrieve the PRHolders
	msgChan chan *MsgNotification
}

func NewMsgNotifier() *Notifier {
	return &Notifier{
		msgChan: make(chan *MsgNotification),
	}
}

func (n *Notifier) GetNotChan() chan *MsgNotification {
	return n.msgChan
}

func (n *Notifier) Notify(msgStatus *MsgNotification) {
	n.msgChan <- msgStatus
}

func (n *Notifier) Close() {
	close(n.msgChan)
}
