package hoarder

import (
	"context"
	log "github.com/sirupsen/logrus"
	"reflect"
	"sync"
	"time"

	src "ipfs-cid-hoarder/pkg/cid-source"
	"ipfs-cid-hoarder/pkg/db"
	"ipfs-cid-hoarder/pkg/p2p"
)

type Tracker interface {
	run()
	//this should be run as a go routine
	generateCids(genWG *sync.WaitGroup, GetNewCidReturnTypeChannel chan<- *src.GetNewCidReturnType)
}

// CidTracker composes the basic object that generates and publishes the set of CIDs defined in the configuration
type CidTracker struct {
	ctx context.Context
	wg  *sync.WaitGroup

	m sync.Mutex

	host      *p2p.Host
	DBCli     *db.DBClient
	CidSource src.CidSource
	CidPinger *CidPinger

	K              int
	Workers        int
	ReqInterval    time.Duration
	StudyDuration  time.Duration
	CidMap         sync.Map
	ProviderAndCID sync.Map
}

type CidPublisher struct {
	//receive message from listen for add provider message function
	MsgNot *p2p.Notifier
	//number of cids to publish
	CidNumber int
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
	k, Workers int,
	reqInterval, studyDuration time.Duration) (*CidTracker, error) {

	return &CidTracker{
		ctx:           ctx,
		host:          h,
		wg:            wg,
		DBCli:         db,
		CidSource:     cidSource,
		CidPinger:     cidPinger,
		K:             k,
		ReqInterval:   reqInterval,
		StudyDuration: studyDuration,
		Workers:       Workers,
	}, nil
}

func (tracker *CidTracker) run() {

}

//Generates cids depending on the cid source
func (tracker *CidTracker) generateCids(genWG *sync.WaitGroup, GetNewCidReturnTypeChannel chan<- *src.GetNewCidReturnType) {
	defer genWG.Done()
	for true {
		cid, err := tracker.CidSource.GetNewCid()
		if reflect.DeepEqual(cid, src.Undef) {
			break
		}
		if err != nil {
			log.Errorf("error while getting new cid: %s", err)
			return
		}
		GetNewCidReturnTypeChannel <- &cid
	}
}
