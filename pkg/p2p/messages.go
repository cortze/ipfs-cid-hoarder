package p2p

import (
	"context"
	"fmt"
	"time"

	net "github.com/libp2p/go-libp2p-kad-dht/net"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// MessageSender handles sending wire protocol messages to a given peer
type MessageSender struct {
	m             pb.MessageSenderWithDisconnect
	blacklistedUA string
	msgNot        *MsgNotifier
}

func NewCustomMessageSender(blacklistedUA string, withMsgNot bool) *MessageSender {
	msgSender := &MessageSender{
		blacklistedUA: blacklistedUA,
	}
	// only generate a notifier if requested
	if withMsgNot {
		msgSender.msgNot = NewMsgNotifier()
	}
	return msgSender
}

func (ms *MessageSender) Init(h host.Host, protocols []protocol.ID) pb.MessageSenderWithDisconnect {
	msgSender := net.NewMessageSenderImpl(h, protocols, ms.blacklistedUA)
	ms.m = msgSender
	fmt.Println(protocols)
	return ms
}

func (ms *MessageSender) GetMsgNotifier() *MsgNotifier {
	return ms.msgNot
}

func (ms *MessageSender) OnDisconnect(ctx context.Context, p peer.ID) {
	ms.m.OnDisconnect(ctx, p)
}

func (ms *MessageSender) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	return ms.m.SendRequest(ctx, p, pmes)
}

// SendMessage is a custom wrapper on top of the pb.MessageSender that sends a given msg to a peer and
// notifies throught the given notification channel of the sent msg status
func (ms *MessageSender) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	startT := time.Now()
	err := ms.m.SendMessage(ctx, p, pmes)
	t := time.Since(startT)

	// only notify if the notifier was enabled
	if ms.msgNot != nil {
		not := &MsgNotification{
			RemotePeer:    p,
			QueryTime:     startT,
			QueryDuration: t,
			Msg:           *pmes,
			Error:         err,
		}
		ms.msgNot.Notify(not)
	}
	return err
}
