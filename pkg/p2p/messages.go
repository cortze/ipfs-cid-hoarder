package p2p

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	net "github.com/libp2p/go-libp2p-kad-dht/net"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/prometheus/common/log"
)

// MessageSender handles sending wire protocol messages to a given peer
type MessageSender struct {
	m             pb.MessageSender
	blacklistedUA string
	msgNot        *Notifier
}

func NewCustomMessageSender(blacklistedUA string) *MessageSender {
	return &MessageSender{
		blacklistedUA: blacklistedUA,
		msgNot:        NewMsgNotifier(),
	}
}
func (ms *MessageSender) Init(h host.Host, protocols []protocol.ID) pb.MessageSender {
	msgSender := net.NewMessageSenderImpl(h, protocols, ms.blacklistedUA)
	ms.m = msgSender
	return ms
}

func (ms *MessageSender) GetMsgNotifier() *Notifier {
	return ms.msgNot
}

// SendRequest is a custom wrapper on top of the pb.MessageSender that sends a peer a message and waits for its response
//TODO check out if this is a response message
//Added by fotis bistas
func (ms *MessageSender) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	// Keep track of the time that takes to send the msg
	startT := time.Now()
	respMsg, err := ms.m.SendRequest(ctx, p, pmes)
	t := time.Since(startT)

	// compose the Notifier
	not := &MsgNotification{
		RemotePeer:    p,
		QueryTime:     startT,
		QueryDuration: t,
		Msg:           *pmes,
		Error:         err,
	}
	log.Debugf(respMsg.Type)
	ms.msgNot.Notify(not)
	return respMsg, err
}

// SendMessage is a custom wrapper on top of the pb.MessageSender that sends a given msg to a peer and
// notifies throught the given notification channel of the sent msg status
func (ms *MessageSender) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	// Keep track of the time that takes to send the msg
	startT := time.Now()
	err := ms.m.SendMessage(ctx, p, pmes)
	t := time.Since(startT)

	// compose the Notifier
	not := &MsgNotification{
		RemotePeer:    p,
		QueryTime:     startT,
		QueryDuration: t,
		Msg:           *pmes,
		Error:         err,
	}
	ms.msgNot.Notify(not)
	return err
}
