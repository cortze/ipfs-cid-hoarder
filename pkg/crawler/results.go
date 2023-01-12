package crawler

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	log "github.com/sirupsen/logrus"
)

const (
	disc      = iota // peer that got discovered but not connected
	active           // peer that got successfully connected
	blacklist        // peer that got connected but belongs to the blacklistable UserAgent List
	failed
)

type CrawlResults struct {
	m          sync.RWMutex
	discPeers  map[peer.ID]int
	initTime   time.Time
	finishTime time.Time
}

func NewCrawlerResults() *CrawlResults {
	return &CrawlResults{
		discPeers: make(map[peer.ID]int),
	}
}

func (r *CrawlResults) addPeer(p peer.ID, status int) {
	r.m.Lock()
	defer r.m.Unlock()

	log.WithFields(log.Fields{
		"peer":   p.String(),
		"status": status,
	}).Debug("new peer discovered")

	// add the peer to the discovered list with the received status

	// if the peer wasn't already in the map, add it straight away
	s, ok := r.discPeers[p]
	if ok {
		// check previous status
		switch s {
		case disc: // if peer was only disc - write straight away
			r.discPeers[p] = status
		case active: // if peer was active - only write if blacklistable?
			if status == blacklist {
				r.discPeers[p] = status
			}
		case blacklist: // if it was blacklisted - do nothing
		case failed:
			if status == active || status == blacklist {
				r.discPeers[p] = status
			}
		default: // do nothing?
		}
	} else {
		// add it straight away
		r.discPeers[p] = status
	}

}

func (r *CrawlResults) GetDiscvPeers() map[peer.ID]struct{} {
	r.m.RLock()
	defer r.m.RUnlock()

	dicv := make(map[peer.ID]struct{})

	for k, _ := range r.discPeers {
		dicv[k] = struct{}{}
	}

	return dicv
}

func (r *CrawlResults) GetActivePeers() map[peer.ID]struct{} {
	r.m.RLock()
	defer r.m.RUnlock()

	atcv := make(map[peer.ID]struct{})

	for k, v := range r.discPeers {
		if v == active {
			atcv[k] = struct{}{}
		}
	}

	return atcv
}

func (r *CrawlResults) GetBlacklistedPeers() map[peer.ID]struct{} {
	r.m.RLock()
	defer r.m.RUnlock()

	blsp := make(map[peer.ID]struct{})

	for k, v := range r.discPeers {
		if v == blacklist {
			blsp[k] = struct{}{}
		}
	}
	return blsp
}

func (r *CrawlResults) GetFailedPeers() map[peer.ID]struct{} {
	r.m.RLock()
	defer r.m.RUnlock()

	fail := make(map[peer.ID]struct{})

	for k, v := range r.discPeers {
		if v == failed {
			fail[k] = struct{}{}
		}
	}
	return fail
}

func (c *CrawlResults) GetCrawlerDuration() time.Duration {
	return c.finishTime.Sub(c.initTime)
}
