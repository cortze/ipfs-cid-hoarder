package models

import (
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// CidInfo contains the basic information of the CID to track.
// It also includes the information about the PRHolders and the fetch results of all the fetch attempts.
type CidInfo struct {
	m sync.Mutex

	CID cid.Cid

	GenTime     time.Time
	ReqInterval time.Duration

	K             int         // Number of K peers that should get the initial PR
	PRHolders     []*PeerInfo // Peers that took the responsability to keep the PR
	PRPingResults []*CidFetchResults
	ContentType   string  // Type of the content that is under the CID (Random, Video, Image...)
	Source        string  // Track where is the content coming from (Random, Cid-File, Bitswap)
	Creator       peer.ID // Peer hosting the content, so far only one (us)

	ProvideTime time.Duration // time that took to publish the provider records
	NextPing    time.Time
	PingCounter int
}

func NewCidInfo(
	id cid.Cid,
	reqInt time.Duration,
	contentType, source string,
	creator peer.ID) *CidInfo {

	return &CidInfo{
		m:           sync.Mutex{},
		CID:         id,
		GenTime:     time.Now(), // fill the CID with the current time
		ReqInterval: reqInt,
		PRHolders:   make([]*PeerInfo, 0),
		ContentType: contentType,
		Source:      source,
		Creator:     creator,
	}
}

func (c *CidInfo) AddProvideTime(reqTime time.Duration) {
	c.m.Lock()
	defer c.m.Unlock()

	c.ProvideTime = reqTime
}

func (c *CidInfo) AddPRHolder(prHolder *PeerInfo) {
	c.m.Lock()
	defer c.m.Unlock()

	c.PRHolders = append(c.PRHolders, prHolder)
	c.K++
}

func (c *CidInfo) AddPRFetchResults(results *CidFetchResults) {
	c.m.Lock()
	defer c.m.Unlock()

	c.PRPingResults = append(c.PRPingResults, results)
	// check if the CID is initialized or not
	if c.NextPing == (time.Time{}) {
		// Update the next ping time to gentime + providetime + req interval
		c.NextPing = c.GenTime.Add(c.ProvideTime).Add(c.ReqInterval)
	}
}

func (c *CidInfo) IncreasePingCounter() {
	c.m.Lock()
	defer c.m.Unlock()

	c.PingCounter++
	c.NextPing = c.NextPing.Add(c.ReqInterval)
}

func (c *CidInfo) GetPingCounter() int {
	c.m.Lock()
	defer c.m.Unlock()

	return c.PingCounter
}

func (c *CidInfo) GetFetchResultsLen() int {
	return len(c.PRPingResults)
}

func (c *CidInfo) GetFetchResultSummaryOfRound(round int) (tot, success, failed int) {
	// check if the Result Array has enough rounds as the requested one
	if round >= c.GetFetchResultsLen() {
		return -1, -1, -1
	}
	// calculate the summary of the PingRound
	cidFetchRes := c.PRPingResults[round]
	return cidFetchRes.GetSummary()
}
