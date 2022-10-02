package hoarder

import (
	"sort"
	"sync"

	"ipfs-cid-hoarder/pkg/models"
)

// cidQueue is a simple queue of CIDs that allows rapid access to content through maps,
// while being abel to sort the array by closer next ping time to determine which is
// the next soonest peer to ping
type cidQueue struct {
	sync.RWMutex

	cidMap   map[string]*models.CidInfo
	cidArray []*models.CidInfo
}

//Creates a new:
//	cidQueue struct {
//		sync.RWMutex
//
//		cidMap   map[string]*models.CidInfo
//		cidArray []*models.CidInfo
//	}
func newCidQueue() *cidQueue {
	return &cidQueue{
		cidMap:   make(map[string]*models.CidInfo),
		cidArray: make([]*models.CidInfo, 0),
	}
}

func (q *cidQueue) isCidAlready(c string) bool {
	q.RLock()
	defer q.RUnlock()

	_, ok := q.cidMap[c]
	return ok
}

func (q *cidQueue) addCid(c *models.CidInfo) {
	q.Lock()
	defer q.Unlock()

	q.cidMap[c.CID.Hash().B58String()] = c
	q.cidArray = append(q.cidArray, c)

	return
}

func (q *cidQueue) removeCid(cStr string) {
	delete(q.cidMap, cStr)
	// check if len of the queue is only one
	if q.Len() == 1 {
		q.cidArray = make([]*models.CidInfo, 0)
		return
	}
	item := -1
	for idx, c := range q.cidArray {
		if c.CID.Hash().B58String() == cStr {
			item = idx
			break
		}
	}
	// check if the item was found
	if item >= 0 {
		q.cidArray = append(q.cidArray[:item], q.cidArray[(item+1):]...)
	}
}

func (q *cidQueue) getCid(cStr string) (*models.CidInfo, bool) {
	q.RLock()
	defer q.RUnlock()

	c, ok := q.cidMap[cStr]
	return c, ok
}

func (q *cidQueue) sortCidList() {
	sort.Sort(q)
	return
}

// Swap is part of sort.Interface.
func (q *cidQueue) Swap(i, j int) {
	q.Lock()
	defer q.Unlock()

	q.cidArray[i], q.cidArray[j] = q.cidArray[j], q.cidArray[i]
}

// Less is part of sort.Interface. We use c.PeerList.NextConnection as the value to sort by.
func (q *cidQueue) Less(i, j int) bool {
	q.RLock()
	defer q.RUnlock()

	return q.cidArray[i].NextPing.Before(q.cidArray[j].NextPing)
}

// Len is part of sort.Interface. We use the peer list to get the length of the array.
func (q *cidQueue) Len() int {
	q.RLock()
	defer q.RUnlock()

	return len(q.cidArray)
}
