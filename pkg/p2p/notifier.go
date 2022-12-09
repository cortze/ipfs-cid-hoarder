package p2p

import (
	"time"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/peer"
)

//This is the struct type received and send by the notifier's channel.
type MsgNotification struct {
	RemotePeer    peer.ID
	QueryTime     time.Time
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

//This returns the channel of the notifier struct:
//
// 	msgChan chan *MsgNotification
//The notifier channel receives and sends:
//	MsgNotification struct{
//		RemotePeer    peer.ID
//		QueryTime     time.Time
//		QueryDuration time.Duration
//		Msg           pb.Message
//		Resp          pb.Message
//		Error         error
//	}
func (notifier_instance *Notifier) GetNotifierChan() chan *MsgNotification {
	return notifier_instance.msgChan
}

//Sends a new:
//	MsgNotification struct{
//	...
//	}
// To the underlying channel:
//	msgChan chan *MsgNotification
func (notifier_instance *Notifier) Notify(msgStatus *MsgNotification) {
	notifier_instance.msgChan <- msgStatus
}

// Closes the underlying:
//	msgChan chan *MsgNotification
func (notifier_instance *Notifier) Close() {
	close(notifier_instance.msgChan)
}
