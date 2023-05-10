package hoarder

import (
	"context"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	"github.com/pkg/errors"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
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

	host         *p2p.Host
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
	hostOpts p2p.HostOptions,
	dbCli *db.DBClient,
	pingInterval time.Duration,
	workers int,
	cidSet *cidSet) (*CidPinger, error) {

	log.WithField("mod", "pinger").Info("initializing the pinger")
	h, err := p2p.NewHost(
		ctx, 
		hostOpts.IP, 
		hostOpts.Port, 
		hostOpts.K, 
		hostOpts.BlacklistingUA, 
		hostOpts.BlacklistedPeers,
	)
	if err != nil {
		return nil, errors.Wrap(err, "pinger:")
	}
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
	pinger.appWG.Add(1)
	defer pinger.appWG.Done()

	logEntry := log.WithField("pinger", "setup")
	// initialize the host
	// TODO: consider having multiple pinger hosts to have concurrency calls:
	// https://github.com/libp2p/go-libp2p-kad-dht/issues/805
	logEntry.Debug("bootstraping host")
	err := pinger.host.Boostrap(pinger.ctx)
	if err != nil {
		logEntry.Panic(errors.Wrap(err, "pinger:"))	
	}

	pinger.orchersterWG.Add(1)
	// Generate the Ping Orchester
	go pinger.runPingOrchester()

	// Launch CID pingers (workers)
	for pingerCounter := 0; pingerCounter < pinger.workers; pingerCounter++ {
		pinger.pingersWG.Add(1)
		go pinger.runPinger(pingerCounter)
	}

	// wait until the ping orchester has finished
	pinger.orchersterWG.Wait()
	logEntry.Infof("finished pinging the CIDs")
	pinger.pingersCloseC <- struct{}{}

	// wait until the ping workers finish
	pinger.pingersWG.Wait()
	logEntry.Info("done from the CID Pinger")

	// close all the pending channels
	close(pinger.pingTaskC)

	//close the publisher host
	err = pinger.host.Close()
	if err != nil {
		logEntry.Errorf("failed to close pinger host: %s", err)
		return
	}
}


// runPingOrchester orchestrates all the pings based on the next ping time of the cids
func (pinger *CidPinger) runPingOrchester() {
	defer pinger.orchersterWG.Done()
	logEntry := log.WithField("pinger", "orchester")
	
	// generate a timer to determine
	minTimeT := time.NewTicker(minIterTime)

	// ensure that the cidSet is not freshly created
	initLoop:
	for !pinger.cidS.isInit() {
		select {
		case<- minTimeT.C:
			minTimeT.Reset(minIterTime)
			logEntry.Trace("cid set still not initialized")
		case <- pinger.orchersterCloseC:
			break initLoop
		}
	}
	orchersterLoop:
	for {
		select {
		case <-pinger.ctx.Done():
			logEntry.Info("shutdown was detected, closing Cid Ping Orchester")
			break orchersterLoop

		case <- pinger.orchersterCloseC:
			logEntry.Info("shutdown was detected, closing Cid Ping Orchester")
			break orchersterLoop

		default:
			// loop over the list of CIDs, and check whether they need to be pinged or not
			orchersterRound:
			for _, cidInfo := range pinger.cidS.cidArray {
				cidStr := cidInfo.CID.Hash().B58String()
				// check if the time for next ping has arrived
				if !cidInfo.IsReadyForNextPing() {
					logEntry.Tracef("not in time to ping %s", cidStr)
					break orchersterRound
				}
				// increment Ping Counter
				cidInfo.IncreasePingCounter()
				// Add the CID to the pingTaskC
				pinger.pingTaskC <- cidInfo
				// check if they have finished the duration of the study
				// if yes, remove them from the cidS.cidArray
				if cidInfo.IsFinished() {
					// delete the CID from the list
					pinger.cidS.removeCid(cidStr)
					logEntry.Infof("finished pinging CID %s - ping time duration reached %s + %s\n", 
						cidStr, cidInfo.PublishTime, cidInfo.StudyDuration,
					)
				}
			}
			// if CID pinger was initialized and there are no more CIDs to track, we are done with the study
			if pinger.cidS.Len() == 0 {
				logEntry.Info("no more cids to ping, closing orcherster")
				break orchersterLoop
			}
			// reorganize the array of CIDs from lowest next ping time to biggest one
			pinger.cidS.sortCidList()
			// check if ticker for next iteration was raised
			<-minTimeT.C
			minTimeT.Reset(minIterTime)
		}
	}
}

// runPinger creates the necessary pinger to retrieve the content from the network and ping the PR holders of the content.
func (pinger *CidPinger) runPinger(pingerID int) {
	defer pinger.pingersWG.Done()

	minTimeT := time.NewTicker(minIterTime)
	logEntry := log.WithField("pinger", pingerID)
	logEntry.Debug("ready")

	closePingerF := false
	for {
		// check if the ping orcherster has finished
		if closePingerF && len(pinger.pingTaskC) == 0 {
			logEntry.Info("no more pingTasks to perform orcherster has finished, finishing worker")
			return
		}
		select {
		case cidInfo := <-pinger.pingTaskC:
			var wg sync.WaitGroup

			cidStr := cidInfo.CID.Hash().B58String()
			pingCounter := cidInfo.GetPingCounter()
			logEntry.Infof("pinging CID %s for round %d", cidStr, pingCounter)

			// request the status of PR Holders
			cidFetchRes := models.NewCidFetchResults(
				cidInfo.CID, 
				cidInfo.PublishTime, 
				pingCounter, 
				cidInfo.K,
			)
			
			// DHT FindProviders call to see if the content is actually retrievable from the network
			wg.Add(1)
			go func(p *CidPinger, c *models.CidInfo, fetchRes *models.CidFetchResults) {
				defer wg.Done()
				var isRetrievable bool = false
				var prWithMAddrs bool = false
				t := time.Now()

				ctxT, cancel := context.WithTimeout(p.ctx, LookupTimeout)
				defer cancel()
				providers, err := p.host.DHT.LookupForProviders(ctxT, c.CID)
				pingTime := time.Since(t)
				fetchRes.FindProvDuration = pingTime
				if err != nil {
					logEntry.Warnf("unable to lookup for provider of cid %s - %s", 
						cidStr, err.Error(),
					)
				}
				// iter through the providers to see if it matches with the host's peerID
				for _, paddrs := range providers {
					if paddrs.ID == c.Creator {
						isRetrievable = true
						if len(paddrs.Addrs) > 0 {
							prWithMAddrs = true
						}
					}
				}
				fetchRes.IsRetrievable = isRetrievable
				fetchRes.PRWithMAddr = prWithMAddrs
			}(pinger, cidInfo, cidFetchRes)

			// recalculate the closest k peers to the content.
			wg.Add(1)
			go func(p *CidPinger, c *models.CidInfo, fetchRes *models.CidFetchResults) {
				defer wg.Done()
				t := time.Now()

				ctxT, cancel := context.WithTimeout(p.ctx, LookupTimeout)
				defer cancel()
				closestPeers, lookupMetrics, err := p.host.DHT.GetClosestPeers(ctxT, string(c.CID.Hash()))
				pingTime := time.Since(t)
				fetchRes.TotalHops = lookupMetrics.GetHops()
				fetchRes.HopsToClosest = lookupMetrics.GetHopsForPeerSet(closestPeers)
				fetchRes.GetClosePeersDuration = pingTime
				if err != nil {
					logEntry.Warnf("unable to get the closest peers to cid %s - %s", cidStr, err.Error())
				}
				for _, peer := range closestPeers {
					cidFetchRes.AddClosestPeer(peer)
				}
			}(pinger, cidInfo, cidFetchRes)

			// Ping in parallel each of the PRHolders
			for _, peerInfo := range cidInfo.PRHolders {
				wg.Add(1)
				go func(wg *sync.WaitGroup, c *models.CidInfo, peerInfo *models.PeerInfo, fetchRes *models.CidFetchResults) {
					defer wg.Done()
					pingRes := pinger.PingPRHolder(c, pingCounter, peerInfo.GetAddrInfo())
					fetchRes.AddPRPingResults(pingRes)
				}(&wg, cidInfo, peerInfo, cidFetchRes)
			}

			// wait until all the parallel routines are done
			wg.Wait()
			// update the finish time for the total fetch round
			cidFetchRes.FinishTime = time.Now()
			// add the fetch results to the array and persist it in the DB
			pinger.dbCli.AddFetchResult(cidFetchRes)

		case <-pinger.ctx.Done():
			logEntry.Info("shutdown detected, closing pinger")
			return
		
		case <- pinger.pingersCloseC:
			logEntry.Info("gracefull shutdown detected")
			closePingerF = true
			return

		case <-minTimeT.C: // and min iter time has passed
			// not ping task to read from the channel, checking again in case we have to close the routine
		}
	}
}

// PingPRHolder dials a given PR Holder to check whether it's active or not, and whether it has the PRs or not
func (pinger *CidPinger) PingPRHolder(c *models.CidInfo, round int, pAddr peer.AddrInfo) *models.PRPingResults {
	logEntry := log.WithFields(log.Fields{
		"ping-to-cid": c.CID.Hash().B58String(),
	})

	var active, hasRecords, recordsWithMAddrs bool
	var connError string

	// track time of the process
	tstart := time.Now()

	// fulfill the control fields from a successful connection
	succesfulConnection := func() {
		active = true
		connError = p2p.NoConnError

		// if the connection was successful, request whether it has the records or not
		provs, _, err := pinger.host.DHT.GetProvidersFromPeer(pinger.ctx, pAddr.ID, c.CID.Hash())
		if err != nil {
			logEntry.Debugf("unable to retrieve providers from peer %s - error: %s", pAddr.ID, err.Error())
		} else {
			logEntry.Debugf("providers for Cid %s from peer %s - %v\n", c.CID.Hash().B58String(), pAddr.ID.String(), provs)
		}
		// iter through the providers to see if it matches with the host's peerID
		for _, paddrs := range provs {
			if paddrs.ID == c.Creator {
				hasRecords = true
				if len(paddrs.Addrs) > 0 {
					recordsWithMAddrs = true
				}
			}
		}
	}

	// check if the peer is among the already connected ones
	if isPeerConnected(pAddr.ID, pinger.host) {
		logEntry.Debugf("peer %s was alread connected, success connection for Cid %s",
			pAddr.ID.String(), c.CID.Hash().B58String(),
		)
		succesfulConnection()
	} else {
		// loop over max tries if the connection is connection refused/ connection reset by peer
		connetionRetry:
		for att := 0; att < maxDialAttempts; att++ {
			// attempt to connect the peer
			err := pinger.host.Connect(pinger.ctx, pAddr)
			connError := p2p.ParseConError(err)
			switch connError {
			case p2p.NoConnError: // no error at all
				logEntry.Debugf("succesful connection to peer %s for Cid %s", 
					pAddr.ID.String(), c.CID.Hash().B58String(),
				)
				succesfulConnection()
				break connetionRetry
		
			case p2p.DialErrorConnectionRefused, p2p.DialErrorStreamReset:
				// the error is due to a connection rejected, try again
				logEntry.Debugf("unable to connect peer %s for Cid %s - error %s, retrying", 
					pAddr.ID.String(), c.CID.Hash().B58String(), err.Error(),
				)
				if (att + 1) < maxDialAttempts {
					// add random delay between trials
					ticker := time.NewTicker(dialGraceTime)
					select {
					case <-ticker.C:
						continue
					case <-pinger.ctx.Done():
						break connetionRetry
					}
				} else {
					logEntry.Infof("%d retries done without succesful connection %s\n", 
						att+1, connError,
					)					
					break connetionRetry
				}

			default:
				// the 
				logEntry.Debugf("unable to connect peer %s for Cid %s - error %s", 
					pAddr.ID.String(), c.CID.Hash().B58String(), err.Error(),
				)
				break connetionRetry
			}
		}
	}

	return models.NewPRPingResults(
		c.CID,
		pAddr.ID,
		round,
		c.PublishTime,
		tstart,
		time.Since(tstart),
		active,
		hasRecords,
		recordsWithMAddrs,
		connError,
	)
}


func (pinger *CidPinger) Close() {
	log.WithField("mod", "pinger").Info("shutdown detected from the CidPinger")
	pinger.orchersterCloseC <- struct{}{}
}

// conside moving this to the Host
func isPeerConnected(pId peer.ID, h host.Host) bool {
	// check if we already have a connection open to the peer
	peerList := h.Network().Peers()
	for _, p := range peerList {
		if p.String() == pId.String() {
			return true
		}
	}
	return false
}


