package hoarder

import (
	"context"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

const (
	LookupTimeout   = 60 * time.Second
	minIterTime     = 500 * time.Millisecond
	maxDialAttempts = 2 // Are two attempts enough?
	dialGraceTime   = 5 * time.Second
)

// CidPinger is the main object to schedule and monitor all the CID related metrics
type CidPinger struct {
	ctx context.Context
	appWG *sync.WaitGroup

	orchersterWG *sync.WaitGroup
	pingersWG *sync.WaitGroup
	orchersterCloseC chan struct{}
	pingersCloseC chan struct{}

	host         *p2p.DHTHost
	dbCli        *db.DBClient
	pingInterval time.Duration
	workers      int

	init            bool

	cidS      *cidSet
	pingTaskC chan *models.CidInfo

}

// NewCidPinger return a CidPinger with the given configuration
func NewCidPinger(
	ctx context.Context,
	appWG *sync.WaitGroup,
	hostOpts p2p.DHTHostOptions,
	dbCli *db.DBClient,
	pingInterval time.Duration,
	workers int,
	cidSet *cidSet) (*CidPinger, error) {

	log.WithField("mod", "pinger").Info("initializing...")
	h, err := p2p.NewDHTHost(
		ctx,
		hostOpts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "pinger:")
	}
	log.WithField("mod", "pinger").Info("initialized...")
	return &CidPinger{
		ctx:          ctx,
		appWG:		  appWG,
		orchersterWG: new(sync.WaitGroup),
		pingersWG: new(sync.WaitGroup),
		orchersterCloseC: make(chan struct{}, 1),
		pingersCloseC: make(chan struct{}, 1),
		host:         h,
		dbCli:        dbCli,
		pingInterval: pingInterval,
		pingTaskC:    make(chan *models.CidInfo, workers), // TODO: hardcoded
		workers:      workers,
		cidS:         cidSet,
	}, nil
}

// Run executes the main logic of the CID Pinger.
// 1. runs the queue logic that schedules the pings
// 2. launchs the pinger pool that will perform all the CID monitoring calls
func (pinger *CidPinger) Run() {
	defer pinger.appWG.Done()

	plog := log.WithField("module", "pinger")

	// TODO: consider having multiple pinger hosts to have concurrency calls:
	// https://github.com/libp2p/go-libp2p-kad-dht/issues/805
	err := pinger.host.Init()
	if err != nil {
		plog.Panic(err)	
	}

	pinger.orchersterWG.Add(1)
	go pinger.runPingOrchester()

	for pingerCounter := 0; pingerCounter < pinger.workers; pingerCounter++ {
		pinger.pingersWG.Add(1)
		go pinger.runPinger(pingerCounter)
	}

	// closing step of the pinger
	pinger.orchersterWG.Wait()
	plog.Infof("finished pinging the CIDs")
	pinger.pingersCloseC <- struct{}{}

	pinger.pingersWG.Wait()
	plog.Info("done from the CID Pinger")

	close(pinger.pingTaskC)
	pinger.host.Close()
}


// runPingOrchester orchestrates all the pings based on the next ping time of the cids
func (pinger *CidPinger) runPingOrchester() {
	defer pinger.orchersterWG.Done()
	olog := log.WithField("pinger", "orchester")
	
	minTimeT := time.NewTicker(minIterTime)
	// ensure that the cidSet is not freshly created
initLoop:
	for !pinger.cidS.isInit() {
		select {
		case<- minTimeT.C:
			olog.Trace("cid set still not initialized")
			minTimeT.Reset(minIterTime)
		case <- pinger.orchersterCloseC:
			break initLoop
		}
	}
orchersterLoop:
	for {
		select {
		case <-pinger.ctx.Done():
			olog.Info("shutdown was detected, closing Cid Ping Orchester")
			break orchersterLoop

		case <- pinger.orchersterCloseC:
			olog.Info("shutdown was detected, closing Cid Ping Orchester")
			break orchersterLoop

		default:
			// loop over the list of CIDs, and check whether they need to be pinged or not
			for _, cidInfo := range pinger.cidS.cidArray {
				cidStr := cidInfo.CID.Hash().B58String()
				if !cidInfo.IsReadyForNextPing() {
					if cidInfo.NextPing.IsZero() {
						// as we organize the cids by ping time, "zero" time gets first
						// breaking always the loop until all the Cids have been generated 
						// and published, thus, let the foor loop find a valid time
						continue
					} else {
						olog.Debugf("not in time to ping %s next ping %s", cidStr, cidInfo.NextPing)
						break
					}
				}

				cidInfo.IncreasePingCounter()
				pinger.pingTaskC <- cidInfo
				if cidInfo.IsFinished() {
					pinger.cidS.removeCid(cidStr)
					olog.Infof("finished pinging CID %s - pingend over %s", 
						cidStr, cidInfo.StudyDuration)
				}
			}
			// if CID pinger was initialized and there are no more CIDs to track, we are done with the study
			if pinger.cidS.Len() == 0 {
				olog.Info("no more cids to ping, closing orcherster")
				break orchersterLoop
			}

			olog.Debugf("reorgananizing (%d) CIDs based on their next ping time", pinger.cidS.Len())
			pinger.cidS.sortCidList()
			<-minTimeT.C
			minTimeT.Reset(minIterTime)
		}
	}
}

// runPinger creates the necessary pinger to retrieve the content from the network and ping the PR holders of the content.
func (pinger *CidPinger) runPinger(pingerID int) {
	defer pinger.pingersWG.Done()

	minTimeT := time.NewTicker(minIterTime)
	plog := log.WithField("pinger", pingerID)
	plog.Debug("ready")

	closePingerF := false
	for {
		// check if the ping orcherster has finished
		if closePingerF && len(pinger.pingTaskC) == 0 {
			plog.Info("no more pingTasks to perform orcherster has finished, finishing worker")
			return
		}
		select {
		case cidInfo := <-pinger.pingTaskC:
			var wg sync.WaitGroup

			cidStr := cidInfo.CID.Hash().B58String()
			pingCounter := cidInfo.GetPingCounter()
			plog.Infof("pinging CID %s for round %d", cidStr, pingCounter)

			// request the status of PR Holders
			cidFetchRes := models.NewCidFetchResults(
				cidInfo.CID, 
				cidInfo.PublishTime, 
				pingCounter, 
				cidInfo.K,
			)
			
			// DHT FindProviders call to see if the content is actually retrievable from the network
			wg.Add(1)
			go func() {
				defer wg.Done()
				var isRetrievable bool = false
				var prWithMAddrs bool = false

				ctxT, cancel := context.WithTimeout(pinger.ctx, LookupTimeout)
				defer cancel()
				queryDuration, providers, err := pinger.host.FindProvidersOfCID(ctxT, cidInfo)
				cidFetchRes.FindProvDuration = queryDuration
				if err != nil {
					plog.Warnf("unable to lookup for provider of cid %s - %s", 
						cidStr, err.Error(),
					)
				}
				// iter through the providers to see if it matches with the host's peerID
				for _, paddrs := range providers {
					if paddrs.ID == cidInfo.Creator {
						isRetrievable = true
						if len(paddrs.Addrs) > 0 {
							prWithMAddrs = true
						}
					}
				}
				cidFetchRes.IsRetrievable = isRetrievable
				cidFetchRes.PRWithMAddr = prWithMAddrs
			}()

			// recalculate the closest k peers to the content.
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctxT, cancel := context.WithTimeout(pinger.ctx, LookupTimeout)
				defer cancel()
				queryDuration, closestPeers, lookupMetrics, err := pinger.host.GetClosestPeersToCid(ctxT, cidInfo)
				if err != nil {
					plog.Warnf("unable to get the closest peers to cid %s - %s", cidStr, err.Error())
					cidFetchRes.TotalHops = -1
					cidFetchRes.HopsTreeDepth = -1
					cidFetchRes.MinHopsToClosest = -1
				} else {
					cidFetchRes.TotalHops = lookupMetrics.GetTotalHops()
					cidFetchRes.HopsTreeDepth = lookupMetrics.GetTreeDepth()
					cidFetchRes.MinHopsToClosest = lookupMetrics.GetMinHopsForPeerSet(lookupMetrics.GetClosestPeers())
				}
				cidFetchRes.GetClosePeersDuration = queryDuration
				for _, peer := range closestPeers {
					cidFetchRes.AddClosestPeer(peer)
				}
			}()

			// Ping in parallel each of the PRHolders
			for _, remotePeer := range cidInfo.PRHolders {
				wg.Add(1)
				go func() {
					defer wg.Done()
					pingRes := pinger.host.PingPRHolderOnCid(
						pinger.ctx, 
						remotePeer.GetAddrInfo(), 
						cidInfo)
					pingRes.Round = pingCounter
					cidFetchRes.AddPRPingResults(pingRes)
				}()
			}
			wg.Wait() // wait for all the routines to finish
			cidFetchRes.FinishTime = time.Now()
			pinger.dbCli.AddFetchResult(cidFetchRes)

		case <-pinger.ctx.Done():
			plog.Info("shutdown detected, closing pinger")
			return
		
		case <- pinger.pingersCloseC:
			plog.Info("gracefull shutdown detected")
			closePingerF = true
			return

		case <-minTimeT.C: // and min iter time has passed
			// not ping task to read from the channel, checking again in case we have to close the routine
		}
	}
}


func (pinger *CidPinger) Close() {
	log.WithField("mod", "pinger").Info("shutdown detected from the CidPinger")
	pinger.orchersterCloseC <- struct{}{}
}

