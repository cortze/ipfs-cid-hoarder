package hoarder

import (
	"context"
	"reflect"
	"sync"
	"time"

	src "ipfs-cid-hoarder/pkg/cid-source"
	"ipfs-cid-hoarder/pkg/db"
	"ipfs-cid-hoarder/pkg/p2p"

	log "github.com/sirupsen/logrus"

	"github.com/ipfs/go-cid"
)

type Tracker interface {
	run()
}

// CidTracker composes the basic object that generates and publishes the set of CIDs defined in the configuration
type CidTracker struct {
	ctx context.Context
	wg  *sync.WaitGroup

	m sync.Mutex

	host      *p2p.Host
	DBCli     *db.DBClient
	MsgNot    *p2p.Notifier
	CidSource src.CidSource
	CidPinger *CidPinger

	K              int
	CidNumber      int
	Workers        int
	ReqInterval    time.Duration
	StudyDuration  time.Duration
	CidMap         sync.Map
	ProviderAndCID sync.Map
}

type CidPublisher struct {
	*CidTracker
}

type CidDiscoverer struct {
	*CidTracker
}

//Creates a new:
//	CidTracker struct{
//		ctx context.Context
//		wg  *sync.WaitGroup
//
//		m sync.Mutex
//
//		host      *p2p.Host
//		DBCli     *db.DBClient
//		MsgNot    *p2p.Notifier
//		CidSource CidSource
//		CidPinger *CidPinger
//
//		K             int
//		CidNumber     int
//		Workers       int
//		ReqInterval   time.Duration
//		StudyDuration time.Duration
//		CidMap        sync.Map
//	}
func NewCidTracker(
	ctx context.Context,
	wg *sync.WaitGroup,
	h *p2p.Host,
	db *db.DBClient,
	cidSource src.CidSource,
	cidPinger *CidPinger,
	k, cidNum, Workers int,
	reqInterval, studyDuration time.Duration) (*CidTracker, error) {

	return &CidTracker{
		ctx:           ctx,
		host:          h,
		wg:            wg,
		DBCli:         db,
		MsgNot:        h.GetMsgNotifier(),
		CidSource:     cidSource,
		CidPinger:     cidPinger,
		K:             k,
		CidNumber:     cidNum,
		ReqInterval:   reqInterval,
		StudyDuration: studyDuration,
		Workers:       Workers,
	}, nil
}

func (tracker *CidTracker) run() {

}

//Generates cids randomly
func (publisher *CidPublisher) generateCids(source src.CidSource, cidNumber int, wg *sync.WaitGroup, cidChannel chan<- *cid.Cid) {
	defer wg.Done()
	// generate the CIDs
	for i := 0; i < cidNumber; i++ {
		GetNewCidReturnTypeInstance, err := source.GetNewCid()
		if err != nil {
			log.Errorf("unable to generate %s content. %s", err.Error(), source.Type())
			continue
		}
		cidChannel <- &GetNewCidReturnTypeInstance.CID
	}
}

//Reads cids from a file
func (discoverer *CidDiscoverer) readCIDs(source src.CidSource, wg *sync.WaitGroup, GetNewCidReturnTypeChannel chan<- *src.GetNewCidReturnType) {
	defer wg.Done()
	for {
		GetNewCidReturnTypeInstance, err := source.GetNewCid()
		if err != nil {
			log.Errorf("unable to read %s content. %s", err.Error(), source.Type())
			continue
		}
		if reflect.DeepEqual(GetNewCidReturnTypeInstance, src.Undef) {
			log.Debug("break from read cids")
			break
		}
		log.Debugf("Get new cid read: cid %s", GetNewCidReturnTypeInstance.CID.String())
		GetNewCidReturnTypeChannel <- &GetNewCidReturnTypeInstance
	}
}
