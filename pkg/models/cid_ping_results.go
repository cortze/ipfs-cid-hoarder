package models

import (
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// PRPingResults is the basic struct containing the result of the individual ping of a PR Holder.
type PRPingResults struct {
	Cid           cid.Cid
	PeerID        peer.ID
	Round         int
	FetchTime     time.Time
	FetchDuration time.Duration
	Active        bool
	HasRecords    bool
	ConError      string
}

func NewPRPingResults(
	cid cid.Cid,
	p peer.ID,
	round int,
	fetchT time.Time,
	fetchD time.Duration,
	active bool,
	hasRecords bool,
	connError string) *PRPingResults {

	return &PRPingResults{
		cid,
		p,
		round,
		fetchT,
		fetchD,
		active,
		hasRecords,
		connError,
	}
}

// CidFetchResults is the basic struct containing the summary of all the requests done for a ficen CID on a fetch round.
type CidFetchResults struct {
	m   sync.Mutex
	Cid cid.Cid

	Round                 int
	StartTime             time.Time
	FinishTime            time.Time
	PRHoldPingDuration    time.Duration
	FindProvDuration      time.Duration
	GetClosePeersDuration time.Duration
	PRPingResults         []*PRPingResults
	IsRetrievable         bool
	ClosestPeers          []peer.ID
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

// AddPRPingResults inserts a new PingResult into the FetchResult, updating at the same time the final duration of the PR Holder ping process
func (c *CidFetchResults) AddPRPingResults(pingRes *PRPingResults) {
	c.m.Lock()
	defer c.m.Unlock()

	c.PRPingResults = append(c.PRPingResults, pingRes)
	if pingRes.FetchDuration > c.PRHoldPingDuration {
		c.PRHoldPingDuration = pingRes.FetchDuration
	}
}

// GetSummary returns the summary of the PR Holder pings for the fetch round
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

// AddClosestPeer inserts into the PRFetchResults a peer that is inside the K closest peers in the IPFS DHT in that fetch round
func (c *CidFetchResults) AddClosestPeer(pInfo peer.ID) {
	c.m.Lock()
	defer c.m.Unlock()

	c.ClosestPeers = append(c.ClosestPeers, pInfo)
}
