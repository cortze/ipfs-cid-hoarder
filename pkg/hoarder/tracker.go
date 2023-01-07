package hoarder

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	src "ipfs-cid-hoarder/pkg/cid-source"
	"ipfs-cid-hoarder/pkg/config"
	"ipfs-cid-hoarder/pkg/db"
	"ipfs-cid-hoarder/pkg/p2p"
)

type Tracker interface {
	run()
	//this should be run as a go routine
	generateCids(genWG *sync.WaitGroup, trackableCidC chan<- *src.TrackableCid)
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
	//receive message from listen for add provider message function
	MsgNot        *p2p.Notifier
	K             int
	Workers       int
	ReqInterval   time.Duration
	StudyDuration time.Duration
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
		MsgNot:        h.GetMsgNotifier(),
		CidSource:     cidSource,
		CidPinger:     cidPinger,
		K:             k,
		ReqInterval:   reqInterval,
		StudyDuration: studyDuration,
		Workers:       Workers,
	}, nil
}

//Generates cids depending on the cid source
func (tracker *CidTracker) generateCids(genWG *sync.WaitGroup, trackableCidC chan<- *src.TrackableCid) {
	defer genWG.Done()
	// generate a timer to determine
	minTimeT := time.NewTicker(5 * time.Second)
	for true {
		cid, err := tracker.CidSource.GetNewCid()
		if cid.IsEmpty() {
			log.Debugf("Received empty cid: %s with peer id: %s", cid.CID.String(), cid.ID.String())
			trackableCidC <- &cid
			close(trackableCidC)
			break
		}
		if tracker.CidSource.Type() == config.HttpServerSource {
			log.Debugf("Source is: %s and config source is: ", tracker.CidSource.Type(), config.HttpServerSource)
			if err != nil {
				log.Errorf("error while getting new cid: %s", err)
				// check if ticker for next iteration was raised
				<-minTimeT.C
				continue
			}
			// check if ticker for next iteration was raised
			<-minTimeT.C
		} else {
			if err != nil {
				log.Errorf("error while getting new cid: %s", err)
				return
			}
		}

		trackableCidC <- &cid
	}
}
