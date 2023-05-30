package p2p

import (
	"time"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)


// MsgNotifier represents the communication channel or the debugging channel
// for the Hoarder to identify the messages sent over the DHT messenger
type MsgNotifier struct {
	msgChan chan *MsgNotification
}

func NewMsgNotifier() *MsgNotifier {
	return &MsgNotifier{
		msgChan: make(chan *MsgNotification),
	}
}

func (n *MsgNotifier) GetNotifierChan() chan *MsgNotification {
	return n.msgChan
}

func (n *MsgNotifier) Notify(msgStatus *MsgNotification) {
	n.msgChan <- msgStatus
}

func (n *MsgNotifier) Close() {
	close(n.msgChan)
}

// MsgNotification is the basic notification struct received and send by the dht messenger
type MsgNotification struct {
	RemotePeer    peer.ID
	QueryTime     time.Time
	QueryDuration time.Duration
	Msg           pb.Message
	Resp          pb.Message
	Error         error
}
