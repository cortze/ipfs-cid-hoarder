package models

import (
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

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

func (c *CidInfo) AddReqTimeInterval(reqTime time.Duration) {
	c.m.Lock()
	defer c.m.Unlock()

	c.ReqInterval = reqTime
}

func (c *CidInfo) AddPRHolder(prHolder *PeerInfo) {
	c.m.Lock()
	defer c.m.Unlock()

	c.PRHolders = append(c.PRHolders, prHolder)
	c.ProvideTime = time.Since(c.GenTime)
	c.K++
}

func (c *CidInfo) AddPRFetchResults(results *CidFetchResults) {
	c.m.Lock()
	defer c.m.Unlock()

	c.PRPingResults = append(c.PRPingResults, results)
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
