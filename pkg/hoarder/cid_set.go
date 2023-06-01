package hoarder

import (
	"sort"
	"sync"

	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
)

// cidSet is a simple s.eue of CIDs that allows rapid access to content through maps,
// while being abel to sort the array by closer next ping time to determine which is
// the next soonest peer to ping
type cidSet struct {
	sync.RWMutex

	cidMap   map[string]*models.CidInfo
	cidArray []*models.CidInfo

	init    bool
	pointer int
}

// newCidQueue creates a new CidSet
func newCidSet() *cidSet {
	return &cidSet{
		cidMap:   make(map[string]*models.CidInfo),
		cidArray: make([]*models.CidInfo, 0),
		init:     false,
		pointer:  -1,
	}
}

func (s *cidSet) isInit() bool {
	return s.init
}

func (s *cidSet) isCidAlready(c string) bool {
	s.RLock()
	defer s.RUnlock()

	_, ok := s.cidMap[c]
	return ok
}

func (s *cidSet) addCid(c *models.CidInfo) {
	s.Lock()
	defer s.Unlock()

	s.cidMap[c.CID.Hash().B58String()] = c
	s.cidArray = append(s.cidArray, c)

	if !s.init {
		s.init = true
	}
}

func (s *cidSet) removeCid(cStr string) {
	delete(s.cidMap, cStr)
	// check if len of the s.eue is only one
	if s.Len() == 1 {
		s.cidArray = make([]*models.CidInfo, 0)
		return
	}
	item := -1
	for idx, c := range s.cidArray {
		if c.CID.Hash().B58String() == cStr {
			item = idx
			break
		}
	}
	// check if the item was found
	if item >= 0 {
		s.cidArray = append(s.cidArray[:item], s.cidArray[(item+1):]...)
	}
}

// Iterators

func (s *cidSet) Next() bool {
	s.RLock()
	defer s.RUnlock()
	return s.pointer < s.Len() && s.pointer >= 0
}

func (s *cidSet) Cid() *models.CidInfo {
	if s.pointer >= s.Len() {
		return nil
	}
	s.Lock()
	defer func() {
		s.pointer++
		s.Unlock()
	}()
	return s.cidArray[s.pointer]
}

// common usage

func (s *cidSet) getCid(cStr string) (*models.CidInfo, bool) {
	s.RLock()
	defer s.RUnlock()

	c, ok := s.cidMap[cStr]
	return c, ok
}

func (s *cidSet) getCidList() []*models.CidInfo {
	s.RLock()
	defer s.RUnlock()
	cidList := make([]*models.CidInfo, s.Len())
	cidList = append(cidList, s.cidArray...)
	return cidList
}

func (s *cidSet) SortCidList() {
	sort.Sort(s)
	s.pointer = 0
	return
}

// Swap is part of sort.Interface.
func (s *cidSet) Swap(i, j int) {
	s.Lock()
	defer s.Unlock()

	s.cidArray[i], s.cidArray[j] = s.cidArray[j], s.cidArray[i]
}

// Less is part of sort.Interface. We use c.PeerList.NextConnection as the value to sort by.
func (s *cidSet) Less(i, j int) bool {
	s.RLock()
	defer s.RUnlock()

	return s.cidArray[i].NextPing.Before(s.cidArray[j].NextPing)
}

// Len is part of sort.Interface. We use the peer list to get the length of the array.
func (s *cidSet) Len() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.cidArray)
}
