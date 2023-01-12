package cid_source

import (
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

var DefCIDContLen = 1024 // 1KB

type CidSource interface {
	GetNewCid() (TrackableCid, error)
	Type() string
}

const layoutPublicationTime = "Jan 2, 2006 at 3:04pm (MST)"

//Encapsulates the return type of the GetNewCid()
type TrackableCid struct {
	ID              peer.ID        `json:"PeerID"`
	CID             cid.Cid        `json:"ContentID"`
	Creator         peer.ID        `json:"Creator"`
	PublicationTime time.Time      `json:"PublicationTime"`
	ProvideTime     time.Duration  `json:"ProvideTime"`
	UserAgent       string         `json:"UserAgent"`
	Content         []byte         `json:"Content"`
	Addresses       []ma.Multiaddr `json:"PeerMultiaddresses"`
}

func NewTrackableCid(ID peer.ID, CID cid.Cid, creator peer.ID, Addresses []ma.Multiaddr, publicationTime time.Time, providetime time.Duration, useragent string) TrackableCid {
	return TrackableCid{
		ID:              ID,
		CID:             CID,
		Creator:         creator,
		ProvideTime:     providetime,
		PublicationTime: publicationTime,
		UserAgent:       useragent,
		Content:         make([]byte, 0),
		Addresses:       Addresses,
	}
}

func (t *TrackableCid) IsEmpty() bool {
	return t == (&TrackableCid{})
}
