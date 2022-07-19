package hoarder

import (
	"context"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	log "github.com/sirupsen/logrus"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

type CidSource interface {
	GetNewCid() ([]byte, cid.Cid, error)
	Type() string
}

// CidTracker composes the basic ojbject that generates and publishes the set of CIDs defined in the configuration
type CidTracker struct {
	ctx context.Context
	wg  *sync.WaitGroup

	m sync.Mutex

	host      *p2p.Host
	DBCli     *db.DBClient
	MsgNot    *p2p.Notifier
	CidSource CidSource
	CidPinger *CidPinger

	K             int
	CidNumber     int
	Workers       int
	ReqInterval   time.Duration
	StudyDuration time.Duration
	CidMap        sync.Map
}

// NewCidTracker returns a CidTracker object from the run parameters
func NewCidTracker(
	ctx context.Context,
	wg *sync.WaitGroup,
	h *p2p.Host,
	db *db.DBClient,
	cidSource CidSource,
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

// Run initializes and starts the run/study
func (t *CidTracker) Run() {
	// generate different run routines for the different cid source methods

	switch t.CidSource.Type() {
	case "random-content-gen":
		t.newRandomCidTracker()

	case "text-file":
		log.Info("initializing Text-File Cid Tracker (still not supported)")
	case "bitswap":
		log.Info("initializing Bitswap Cid Tracker (still not supported)")
	default:
		log.Errorf("cid source method not defined. cid source method = %s", t.CidSource.Type())

	}
}

// newRandomCidTracker runs a randome CID tracker obj
func (t *CidTracker) newRandomCidTracker() {
	defer t.wg.Done()

	log.Info("launching the Random Cid Tracker")

	// launch the PRholder reading routine
	msgNotC := t.MsgNot.GetNotChan()

	// generate a channel with the same size as the Workers one
	cidC := make(chan *cid.Cid, t.Workers)

	var firstCidFetchRes sync.Map

	// IPFS DHT Message Notification Listener
	done := make(chan struct{})
	go func() {

		for {
			select {
			case msgNot := <-msgNotC:
				if msgNot == nil {
					log.Warn("empty msgNot Received, closing reader for PR Holders")
					return
				}
				c, err := cid.Cast(msgNot.Msg.GetKey())
				if err != nil {
					log.Error("unable to cast msg key into cid. %s", err.Error())
				}
				switch msgNot.Msg.Type {
				case pb.Message_ADD_PROVIDER:
					var active, hasRecords bool
					var connError string

					if msgNot.Error != nil {
						connError = p2p.ParseConError(msgNot.Error) //TODO: parse the errors in a better way
						log.Debugf("Failed putting PR for CID %s of PRHolder %s - error %s", c.String(), msgNot.RemotePeer.String(), msgNot.Error.Error())
					} else {
						active = true
						hasRecords = true
						connError = p2p.NoConnError
						log.Debugf("Successfull PRHolder for CID %s of PRHolder %s", c.String(), msgNot.RemotePeer.String())
					}

					pingRes := models.NewPRPingResults(
						c,
						msgNot.RemotePeer,
						0, // round is 0 since is the ADD_PROVIDE result
						msgNot.QueryTime,
						msgNot.QueryDuration,
						active,
						hasRecords,
						connError)

					// add the ping result
					val, ok := firstCidFetchRes.Load(c.Hash().B58String())
					cidFetRes := val.(*models.CidFetchResults)
					if !ok {
						log.Errorf("CidFetcher not ready for cid %s", c.Hash().B58String())
					} else {
						// TODO: move into a seaparate method to make the DB interaction easier?
						cidFetRes.AddPRPingResults(pingRes)
					}

					useragent := t.host.GetUserAgentOfPeer(msgNot.RemotePeer)

					// Generate the new PeerInfo struct for the new PRHolder
					prHolderInfo := models.NewPeerInfo(
						msgNot.RemotePeer,
						t.host.Peerstore().Addrs(msgNot.RemotePeer),
						useragent,
					)

					// add PRHolder to the CidInfo
					val, ok = t.CidMap.Load(c.Hash().B58String())
					cidInfo := val.(*models.CidInfo)
					if !ok {
						log.Warnf("unable to find CidInfo on CidMap for Cid %s", c.Hash().B58String())
					} else {
						cidInfo.AddPRHolder(prHolderInfo)
					}

				default:
					log.Debug("msg is not ADD_PROVIDER msg")
				}

			case <-t.ctx.Done():
				log.Info("context has been closed, finishing Random Cid Tracker")
				return

			case <-done:
				log.Info("all the CIDs have been generated")
				return
			}
		}
	}()

	// CID generator
	var genWG sync.WaitGroup
	genWG.Add(1)
	go func(t *CidTracker, wg *sync.WaitGroup, cidC chan *cid.Cid) {
		defer wg.Done()
		// generate the CIDs
		for i := 0; i < t.CidNumber; i++ {
			_, contID, err := t.CidSource.GetNewCid()
			if err != nil {
				log.Errorf("unable to generate random content. %s", err.Error())
			}
			cidC <- &contID
		}
	}(t, &genWG, cidC)

	var publisherWG sync.WaitGroup

	// CID PR Publishers
	for publisher := 0; publisher < t.Workers; publisher++ {
		publisherWG.Add(1)
		// launch publisher
		go func(ctx context.Context, publisherWG *sync.WaitGroup, publisherID int, cidC chan *cid.Cid, cidFetchRes *sync.Map) {
			defer publisherWG.Done()
			logEntry := log.WithField("publisherID", publisherID)
			logEntry.Debugf("publisher ready")
			for {
				select {
				case c := <-cidC:
					if c == nil {
						logEntry.Warn("received empty CID to track, closing publisher")
						// not needed
						return
					}
					logEntry.Debugf("new cid to publish %s", c.Hash().B58String())
					cStr := c.Hash().B58String()
					// generate the new CidInfo
					cidInfo := models.NewCidInfo(*c, t.ReqInterval, config.RandomContent, config.RandomSource, t.host.ID())

					// generate the cidFecher
					t.CidMap.Store(cStr, cidInfo)

					// generate a new CidFetchResults
					fetchRes := models.NewCidFetchResults(*c, 0) // First round = Publish PR
					cidFetchRes.Store(cStr, fetchRes)

					//
					var hops int32
					ctx := context.WithValue(t.ctx, dht.ContextKey("hops"), &hops)
					tstart := time.Now()
					err := t.host.DHT.Provide(ctx, *c, true)
					if err != nil {
						logEntry.Errorf("unable to Provide random content. %s", err.Error())
					}
					reqTime := time.Since(tstart)

					// TODO: Add the number of hops to the SQL database

					// TODO: fix this little wait to comput last PR Holder status
					// little not inside the CID to notify when k peers where recorded?
					time.Sleep(500 * time.Millisecond)

					// add the request time to the CidInfo
					cidInfo.AddProvideTime(reqTime)
					cidInfo.AddPRFetchResults(fetchRes)

					// the Cid has already being published to the network, we can already save it into the DB
					// ----- Persist inot DB -------
					// Add the cidInfo to the DB
					t.DBCli.AddCidInfo(cidInfo)

					// Add the fetchResults to the DB
					t.DBCli.AddFetchResult(fetchRes)
					// ----- End of the persist into DB -------

					// Calculate success ratio on adding PR into PRHolders
					tot, success, failed := cidInfo.GetFetchResultSummaryOfRound(0)
					if tot < 0 {
						logEntry.Warnf("no ping results for the PR provide round of Cid %s", cidInfo.CID.Hash().B58String())
					} else {
						logEntry.Infof("Cid %s - %d total PRHolders | %d successfull PRHolders | %d failed PRHolders",
							c, tot, success, failed)
					}

					// send the cid_info to the cid_pinger adn start pinging PR Holders
					t.CidPinger.AddCidInfo(cidInfo)

				case <-ctx.Done():
					logEntry.WithField("publisherID", publisherID).Debugf("shutdown detected, closing publisher")
					return
				}
			}

		}(t.ctx, &publisherWG, publisher, cidC, &firstCidFetchRes)
	}

	genWG.Wait()
	// check if there are still CIDs to generate
	// loss of time or CPU cicles?
	for {
		if len(cidC) != 0 {
			continue
		} else {
			close(cidC)
			break
		}
	}

	// wait untill all the workers finished generating the CIDs
	publisherWG.Wait()

	// close Msg Notifier
	close(msgNotC)
}
