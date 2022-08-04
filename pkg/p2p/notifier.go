package p2p

import (
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

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
func (notifier_instance *Notifier) GetNotifierChan() chan *MsgNotification {
	return notifier_instance.msgChan
}

func (notifier_instance *Notifier) Notify(msgStatus *MsgNotification) {
	notifier_instance.msgChan <- msgStatus
}

func (notifier_instance *Notifier) Close() {
	close(notifier_instance.msgChan)
}
