package models

import (
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PRPingResults is the basic struct containing the result of the individual ping of a PR Holder.
type PRPingResults struct {
	Cid          cid.Cid
	PeerID       peer.ID
	Round        int
	cidPubTime   time.Time
	PingTime     time.Time
	PingDuration time.Duration
	Active       bool
	HasRecords   bool
	RecordsWithMAddrs bool
	ConError     string
}

// NewPRPingResults creates a new struct with the basic status/performance info for each individual pings to PR Holders
func NewPRPingResults(
	cid cid.Cid,
	p peer.ID,
	round int,
	cidPubTime time.Time,
	pingTime time.Time,
	pingDuration time.Duration,
	active bool,
	hasRecords bool,
	recordsWithMAddrs bool,
	connError string) *PRPingResults {

	return &PRPingResults{
		cid,
		p,
		round,
		cidPubTime,
		pingTime,
		pingDuration,
		active,
		hasRecords,
		recordsWithMAddrs,
		connError,
	}
}

// GetPingTimeSincePublication return the time range since the publication of the CID when we pinged the CID
func (ping *PRPingResults) GetPingTimeSincePublication() time.Duration {
	return ping.PingTime.Add(ping.PingDuration).Sub(ping.cidPubTime)
}

// CidFetchResults is the basic struct containing the summary of all the requests done for a given CID on a fetch round.
type CidFetchResults struct {
	m                     sync.RWMutex
	Cid                   cid.Cid
	cidPubTime            time.Time // time when the CID was published
	Round                 int
	StartTime             time.Time
	FinishTime            time.Time
	TotalHops             int
	HopsToClosest         int
	PRHoldPingDuration    time.Duration
	FindProvDuration      time.Duration
	GetClosePeersDuration time.Duration
	PRPingResults         []*PRPingResults
	IsRetrievable         bool
	PRWithMAddr bool
	ClosestPeers          []peer.ID
	Target int
	DoneC chan struct{}
}

// NewCidFetchResults return the FetchResults struct that contains the basic information for the entire fetch round of a particular CID.
func NewCidFetchResults(contentID cid.Cid, pubTime time.Time, round int, target int) *CidFetchResults {
	return &CidFetchResults{
		Cid:           contentID,
		Round:         round,
		cidPubTime:    pubTime,
		StartTime:     time.Now(),
		FinishTime:    time.Now(),
		PRPingResults: make([]*PRPingResults, 0),
		ClosestPeers:  make([]peer.ID, 0),
		Target: target, // K
		DoneC: make(chan struct{}, 1),
	}
}

// IsDone reports whether the entire Fetch Result has been completed
func (c *CidFetchResults) IsDone() bool {
	return len(c.PRPingResults) >= c.Target
}

// AddPRPingResults inserts a new ping result
// updating at the same time the final duration of the PR Holder ping process.
func (c *CidFetchResults) AddPRPingResults(pingRes *PRPingResults) {
	c.m.Lock()
	defer c.m.Unlock()

	c.PRPingResults = append(c.PRPingResults, pingRes)
	if pingRes.PingDuration > c.PRHoldPingDuration {
		c.PRHoldPingDuration = pingRes.PingDuration
	}
}

func (c *CidFetchResults) GetPublicationTime() time.Time {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.cidPubTime
}

// GetPingTimeSincePublication return the time range since the publication of the CID when we pinged the CID
func (c *CidFetchResults) GetFetchTimeSincePublication() time.Duration {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.FinishTime.Sub(c.cidPubTime)
}

// GetSummary returns the summary of the PR Holder pings for the fetch round
func (c *CidFetchResults) GetSummary() (tot, success, failed int) {
	c.m.RLock()
	defer c.m.RUnlock()
	// calculate the summary of the PingRound
	for _, pingRes := range c.PRPingResults {
		tot++
		if pingRes.Active {
			success++
		} else {
			failed++
		}
	}
	return tot, success, failed
}

// AddClosestPeer inserts into the CidFetchResults a peer that is inside the K closest peers in the IPFS DHT in that fetch round.
func (c *CidFetchResults) AddClosestPeer(pInfo peer.ID) {
	c.m.Lock()
	defer c.m.Unlock()

	c.ClosestPeers = append(c.ClosestPeers, pInfo)
}
