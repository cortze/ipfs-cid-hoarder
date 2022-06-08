package models

import (
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// PRReqState correspond to the basic state that contains the Provider Records Request state
// every time we request the state of a peer that is supposed to keep the PR of a CID in the list
// we will fill up this state
type PRPingResults struct {
	Cid        cid.Cid
	PeerID     peer.ID
	Round      int
	FetchTime  time.Duration
	Active     bool
	HasRecords bool
	ConError   string
}

func NewPRPingResults(
	cid cid.Cid,
	p peer.ID,
	round int,
	fetchT time.Duration,
	active bool,
	hasRecords bool,
	connError string) *PRPingResults {

	return &PRPingResults{
		cid,
		p,
		round,
		fetchT,
		active,
		hasRecords,
		connError,
	}
}

type CidFetchResults struct {
	m   sync.Mutex
	Cid cid.Cid

	Round         int
	StartTime     time.Time
	FinishTime    time.Time
	PRPingResults []*PRPingResults
	IsRetrievable bool // results of the CidLookup (to check if the content is still reachable)
	// TODO: 	-Add the new closest peers to the content? (to track the degradation of the Provider Record)
	ClosestPeers []peer.ID
}

func NewCidFetchResults(contentID cid.Cid, round int) *CidFetchResults {
	return &CidFetchResults{
		m:             sync.Mutex{},
		Cid:           contentID,
		Round:         round,
		StartTime:     time.Now(),
		FinishTime:    time.Now(),
		PRPingResults: make([]*PRPingResults, 0),
		ClosestPeers:  make([]peer.ID, 0),
	}
}

func (c *CidFetchResults) AddPRPingResults(pingRes *PRPingResults) {
	c.m.Lock()
	defer c.m.Unlock()

	c.PRPingResults = append(c.PRPingResults, pingRes)
	t := c.StartTime.Add(pingRes.FetchTime)
	if c.FinishTime.Before(t) {
		c.FinishTime = t
	}
}

func (c *CidFetchResults) GetSummary() (tot, success, failed int) {
	// calculate the summary of the PingRound
	for _, pingRes := range c.PRPingResults {
		tot++
		if pingRes.Active {
			success++
			// add IsRetrievable to true
		} else {
			failed++
		}
	}
	return tot, success, failed
}

func (c *CidFetchResults) AddClosestPeer(pInfo peer.ID) {
	c.m.Lock()
	defer c.m.Unlock()

	c.ClosestPeers = append(c.ClosestPeers, pInfo)
}
