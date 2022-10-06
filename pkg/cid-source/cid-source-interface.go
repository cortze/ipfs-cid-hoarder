package cid_source

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

var DefCIDContLen = 1024 // 1KB

type CidSource interface {
	GetNewCid() (GetNewCidReturnType, error)
	Type() string
}

//Encapsulates the return type of the GetNewCid()
type GetNewCidReturnType struct {
	ID        peer.ID        `json:"PeerID"`
	CID       cid.Cid        `json:"ContentID"`
	Content   []byte         `json:"Content"`
	Addresses []ma.Multiaddr `json:"PeerMultiaddresses"`
}

var Undef = GetNewCidReturnType{}

func NewGetNewCidReturnType(ID peer.ID, CID cid.Cid, Addresses []ma.Multiaddr) GetNewCidReturnType {
	return GetNewCidReturnType{
		ID:        ID,
		CID:       CID,
		Content:   make([]byte, 0),
		Addresses: Addresses,
	}
}
