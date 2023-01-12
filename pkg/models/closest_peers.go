package models

import (
	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type ClosestPeers struct {
	Cid       cid.Cid
	PingRound int
	Peers     []peer.ID
}
