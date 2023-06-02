package hoarder

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

const (
	pingTimeout   = 2 * 60 * time.Second
	minIterTime   = 500 * time.Millisecond
	dialGraceTime = 5 * time.Second
)

// CidPinger is the main object to schedule and monitor all the CID related metrics
type CidPinger struct {
	ctx   context.Context
	appWG *sync.WaitGroup

	orchersterWG     *sync.WaitGroup
	pingersWG        *sync.WaitGroup
	orchersterCloseC chan struct{}
	pingersCloseC    chan struct{}

	hostPool     *p2p.HostPool
	dbCli        *db.DBClient
	pingInterval time.Duration
	workers      int

	cidS          *cidSet
	pingTaskC     chan pingTask
	closePingerCs []chan struct{}
}

type pingTask struct {
	host *p2p.DHTHost
	*models.CidInfo
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
	// Hack: multiple pinger hosts to have concurrency calls:
	// https://github.com/libp2p/go-libp2p-kad-dht/issues/805
	hostPool, err := p2p.NewHostPool( // hosts are already bootstrapped
		ctx,
		(workers/5)+1,
		hostOpts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "pinger:")
	}
	log.WithField("mod", "pinger").Info("initialized...")
	return &CidPinger{
		ctx:              ctx,
		appWG:            appWG,
		orchersterWG:     new(sync.WaitGroup),
		pingersWG:        new(sync.WaitGroup),
		orchersterCloseC: make(chan struct{}, 1),
		pingersCloseC:    make(chan struct{}, 1),
		hostPool:         hostPool,
		dbCli:            dbCli,
		pingInterval:     pingInterval,
		pingTaskC:        make(chan pingTask, workers),
		workers:          workers,
		cidS:             cidSet,
		closePingerCs:    make([]chan struct{}, 0, workers),
	}, nil
}

// Run executes the main logic of the CID Pinger.
// 1. runs the queue logic that schedules the pings
// 2. launchs the pinger pool that will perform all the CID monitoring calls
func (pinger *CidPinger) Run() {
	defer pinger.appWG.Done()

	plog := log.WithField("service", "pinger")

	pinger.orchersterWG.Add(1)
	go pinger.runPingOrchester()

	for pingerCounter := 0; pingerCounter < pinger.workers; pingerCounter++ {
		pinger.pingersWG.Add(1)
		closeC := make(chan struct{})
		go pinger.runPinger(pingerCounter, closeC)
		pinger.closePingerCs = append(pinger.closePingerCs, closeC)
	}

	// closing step of the pinger
	pinger.orchersterWG.Wait()
	plog.Infof("finished pinging the CIDs")
	for _, closeC := range pinger.closePingerCs {
		closeC <- struct{}{}
	}

	pinger.pingersWG.Wait()
	plog.Info("finished the pinging phase")

	close(pinger.pingTaskC)
	pinger.hostPool.Close()
	plog.Info("successfully closed")
}

// runPingOrchester orchestrates all the pings based on the next ping time of the cids
func (pinger *CidPinger) runPingOrchester() {
	defer pinger.orchersterWG.Done()
	olog := log.WithField("pinger", "orchester")

	minTimeT := time.NewTicker(minIterTime)

	// ensure that the cidSet is not freshly created
	for !pinger.cidS.isInit() {
		select {
		case <-minTimeT.C:
			olog.Trace("cid set still not initialized")
			minTimeT.Reset(minIterTime)
		case <-pinger.orchersterCloseC:
			break
		}
	}

	// orchester loop
	for {
		select {
		case <-pinger.ctx.Done():
			olog.Info("shutdown was detected, closing Cid Ping Orchester")
			return

		case <-pinger.orchersterCloseC:
			olog.Info("shutdown was detected, closing Cid Ping Orchester")
			return

		default:
			// loop over the list of CIDs, and check whether they need to be pinged or not
			sortSet := false
			if pinger.cidS.Next() {
				cidInfo := pinger.cidS.Cid()
				if cidInfo == nil {
					sortSet = true
					goto updateCidSet
				}
				cidStr := cidInfo.CID.Hash().B58String()
				if !cidInfo.IsReadyForNextPing() {
					if cidInfo.NextPing.IsZero() {
						// as we organize the cids by ping time, "zero" time gets first
						// breaking always the loop until all the Cids have been generated
						// and published, thus, let the foor loop find a valid time
						goto updateCidSet
					} else {
						olog.Debugf("not in time to ping %s next ping in %s", cidStr, cidInfo.NextPing.Sub(time.Now()))
						sortSet = true
						goto updateCidSet
					}
				}
				cidInfo.IncreasePingCounter()
				h, err := pinger.hostPool.GetBestHost(cidInfo)
				switch err {
				case nil:
					olog.Debug(fmt.Sprintf("got host %d for next ping (%d) ongoing pings",
						h.GetHostID(), h.GetOngoingCidPings()))
				case p2p.ErrorRetrievingBestHost:
					olog.Warn(err)
				}

				if cidInfo.IsFinished() {
					pinger.cidS.removeCid(cidStr)
					olog.Infof("finished pinging CID %s - pingend over %s (%d remaining)",
						cidStr,
						cidInfo.StudyDuration,
						pinger.cidS.Len())
				}
				pinger.pingTaskC <- pingTask{h, cidInfo}

			} else {
				sortSet = true
			}

		updateCidSet:
			// if there are no more CIDs to track, we are done with the study
			if pinger.cidS.Len() == 0 {
				olog.Info("no more cids to ping, closing orcherster")
				return
			}
			// check if we need to update the CidSet
			if sortSet {
				olog.Debugf("reorgananizing (%d) CIDs based on their next ping time", pinger.cidS.Len())
				pinger.cidS.SortCidList()
				<-minTimeT.C
				minTimeT.Reset(minIterTime)
			}
		}
	}
}

// runPinger creates the necessary pinger to retrieve the content from the network and ping the PR holders of the content.
func (pinger *CidPinger) runPinger(pingerID int, closeC chan struct{}) {
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
		case pingT := <-pinger.pingTaskC:
			var wg sync.WaitGroup

			cidStr := pingT.CID.Hash().B58String()
			pingCounter := pingT.GetPingCounter()
			plog.Infof("pinging CID %s for round %d with host %d", cidStr, pingCounter, pingT.host.GetHostID())

			// request the status of PR Holders
			cidFetchRes := models.NewCidFetchResults(
				pingT.CID,
				pingT.PublishTime,
				pingCounter,
				pingT.K,
			)

			pingCtx, cancel := context.WithTimeout(pinger.ctx, pingTimeout)
			defer cancel()
			// DHT FindProviders call to see if the content is actually retrievable from the network
			wg.Add(1)
			go func() {
				defer wg.Done()
				var isRetrievable bool = false
				var prWithMAddrs bool = false

				queryDuration, providers, err := pingT.host.FindProvidersOfCID(pingCtx, pingT.CidInfo)
				cidFetchRes.FindProvDuration = queryDuration
				if err != nil {
					plog.Warnf("unable to lookup for provider of cid %s - %s",
						cidStr, err.Error(),
					)
				}
				// iter through the providers to see if it matches with the host's peerID
				for _, paddrs := range providers {
					if paddrs.ID == pingT.Creator {
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
				queryDuration, closestPeers, lookupMetrics, err := pingT.host.GetClosestPeersToCid(pingCtx, pingT.CidInfo)
				if err != nil {
					plog.Warnf("unable to get the closest peers to cid %s - %s", cidStr, err.Error())
				}
				// just because we got an error, doesn't necesarelly mean that there are no lookup results
				if lookupMetrics == nil {
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
			for _, remotePeer := range pingT.CidInfo.PRHolders {
				wg.Add(1)
				go func(remotePeer models.PeerInfo) {
					defer wg.Done()
					pingRes := pingT.host.PingPRHolderOnCid(
						pingCtx,
						remotePeer.GetAddrInfo(),
						pingT.CidInfo)
					pingRes.Round = pingCounter
					cidFetchRes.AddPRPingResults(pingRes)
				}(*remotePeer)
			}
			wg.Wait() // wait for all the routines to finish
			cidFetchRes.FinishTime = time.Now()
			pinger.dbCli.AddFetchResult(cidFetchRes)

		case <-pinger.ctx.Done():
			plog.Info("shutdown detected, closing pinger")
			return

		case <-closeC:
			plog.Info("gracefull shutdown detected")
			closePingerF = true
			return

		case <-minTimeT.C: // and min iter time has passed
			// not ping task to read from the channel, checking again in case we have to close the routine
		}
	}
}

func (pinger *CidPinger) GetHostWorkload() map[int]int {
	return pinger.hostPool.GetHostWorkload()
}

func (pinger *CidPinger) Close() {
	log.WithField("mod", "pinger").Info("shutdown detected from the CidPinger")
	pinger.orchersterCloseC <- struct{}{}
}
