package models

import (
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// CidInfo contains the basic information of the CID to track.
// It also includes the information about the PRHolders and the fetch results of all the fetch attempts.
type CidInfo struct {
	m sync.RWMutex

	CID cid.Cid

	GenTime     time.Time
	PublishTime time.Time
	ProvideTime time.Duration // time that took to publish the provider records

	K             int         // Number of K peers that should get the initial PR
	PRHolders     []*PeerInfo // Peers that took the responsability to keep the PR
	PRPingResults []*CidFetchResults

	ProvideOp  string // Track where is the content coming from (Random, Cid-File, Bitswap)
	Creator peer.ID // Peer hosting the content, so far only one (us)

	ReqInterval   time.Duration
	StudyDuration time.Duration
	NextPing      time.Time
	pingCounter   int
}

// NewCidInfo creates the basic CID info struct that covers all the metadata and details of the
// provided CIDs
func NewCidInfo(
	id cid.Cid,
	k int,
	reqInt time.Duration,
	studyDurt time.Duration,
	provOp string,
	creator peer.ID) *CidInfo {

	return &CidInfo{
		CID:           id,
		GenTime:       time.Now(), // fill the CID with the current time
		K: 0,
		PRPingResults: make([]*CidFetchResults, 0, k),
		PRHolders:     make([]*PeerInfo, 0),
		ProvideOp:      provOp,
		Creator:       creator,
		ReqInterval:   reqInt,
		StudyDuration: studyDurt,
	}
}

// IsInit return a boolean depending on whether the 
func (c *CidInfo) IsInit() bool {
	return len(c.PRPingResults) > 0 
}

// AddPublicationTime adds the time at which the CID was published to the network
func (c *CidInfo) AddPublicationTime(pubTime time.Time) {
	c.m.Lock()
	defer c.m.Unlock()
	c.PublishTime = pubTime
}

// AddProvideTime modifies the time that took to publish a CID
func (c *CidInfo) AddProvideTime(reqTime time.Duration) {
	c.m.Lock()
	defer c.m.Unlock()
	c.ProvideTime = reqTime
}

// AddCreator aggregates the Peer.ID of the host/client that created the CID
func (c *CidInfo) AddCreator(creator peer.ID) {
	c.m.Lock()
	defer c.m.Unlock()
	c.Creator = creator
}

// AddPRHolder inserts a given Peer selected/attempted to keep the PRs for a CID
func (c *CidInfo) AddPRHolder(prHolder *PeerInfo) {
	c.m.Lock()
	defer c.m.Unlock()
	c.PRHolders = append(c.PRHolders, prHolder)
	c.K++
}

// AddPRFetchResults inserts the results of a given CidFetch round into the CID struct
func (c *CidInfo) AddPRFetchResults(results *CidFetchResults) {
	c.m.Lock()
	defer c.m.Unlock()

	c.PRPingResults = append(c.PRPingResults, results)
	// check if the CID is initialized or not
	if c.NextPing.IsZero() {
		// Update the next ping time to PublicationTime + req interval
		// take also into account the publication time
		offset := c.ProvideTime / 2
		c.NextPing = c.PublishTime.Add(offset).Add(c.ReqInterval)
	}
}

// IsReadyForNextPing returns true if it's time to ping the CID
// (taking into account the nextPing time previously calculated)
func (c *CidInfo) IsReadyForNextPing() bool {
	c.m.RLock()
	defer c.m.RUnlock()
	return !c.NextPing.IsZero() && time.Now().After(c.NextPing) 
}

// IsFinished returns true if the study for the given CID has already finished (enough ping rounds to cover the study time)
// (taking into account the publicationTime previously calculated)
func (c *CidInfo) IsFinished() bool {
	c.m.RLock()
	defer c.m.RUnlock()
	return time.Now().After(c.PublishTime.Add(c.StudyDuration))
}

// IncreasePingCounter increases the internal ping counter, later used to track the ping round
// it also increases the time for the next ping
func (c *CidInfo) IncreasePingCounter() {
	c.m.Lock()
	defer c.m.Unlock()
	c.pingCounter++
	c.NextPing = c.NextPing.Add(c.ReqInterval)
}

// GetPingCounter returns the state of the internal pingCounter
func (c *CidInfo) GetPingCounter() int {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.pingCounter
}

// GetFetchResultsLen returns the number of FetchResults that are stored in the CID
func (c *CidInfo) GetFetchResultsLen() int {
	c.m.RLock()
	defer c.m.RUnlock()
	return len(c.PRPingResults)
}

// GetFetchResultSummaryOfRound returns the summary of a given fetch/ping round of the CID
func (c *CidInfo) GetFetchResultSummaryOfRound(round int) (tot, success, failed int) {
	// check if the Result Array has enough rounds as the requested one
	if round >= c.GetFetchResultsLen() {
		return -1, -1, -1
	}
	// calculate the summary of the PingRound
	c.m.Lock()
	cidFetchRes := c.PRPingResults[round]
	c.m.Unlock()
	return cidFetchRes.GetSummary()
}

func (c *CidInfo) NumberOfPRHolders() int {
	c.m.RLock()
	defer c.m.RUnlock()
	return len(c.PRHolders)
}

func (c *CidInfo) PublicationFinished() bool {
	c.m.RLock()
	defer c.m.RUnlock()
	return len(c.PRHolders) == c.K
}
