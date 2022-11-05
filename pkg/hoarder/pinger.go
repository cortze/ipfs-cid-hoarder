package hoarder

import (
	"context"
	"sync"
	"time"

	"ipfs-cid-hoarder/pkg/db"
	"ipfs-cid-hoarder/pkg/models"
	"ipfs-cid-hoarder/pkg/p2p"

	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	log "github.com/sirupsen/logrus"
)

const (
	DialTimeout     = 20 * time.Second
	minIterTime     = 2 * time.Second
	maxDialAttempts = 3 // Are three attempts enough?
	dialGraceTime   = 10 * time.Second
)

// CidPinger is the main object to schedule and monitor all the CID related metrics
type CidPinger struct {
	ctx context.Context
	sync.Mutex
	wg *sync.WaitGroup

	host         *p2p.Host
	dbCli        *db.DBClient
	pingInterval time.Duration
	rounds       int
	workers      int

	init            bool
	pingOrcDoneFlag bool
	initC           chan struct{}

	cidQ      *cidQueue
	pingTaskC chan *models.CidInfo
}

// NewCidPinger return a CidPinger with the given configuration,creates a new:
//	CidPinger struct {
//		ctx context.Context
//		sync.Mutex
//		wg *sync.WaitGroup
//
//		host         *p2p.Host
//		dbCli        *db.DBClient
//		pingInterval time.Duration
//		rounds       int
//		workers      int
//
//		init  bool
//		initC chan struct{}
//
//		cidQ      *cidQueue
//		pingTaskC chan *models.CidInfo
//	}
func NewCidPinger(
	ctx context.Context,
	wg *sync.WaitGroup,
	host *p2p.Host,
	dbCli *db.DBClient,
	pingInterval time.Duration,
	rounds int,
	workers int) *CidPinger {

	return &CidPinger{
		ctx:             ctx,
		wg:              wg,
		host:            host,
		dbCli:           dbCli,
		pingInterval:    pingInterval,
		rounds:          rounds,
		cidQ:            newCidQueue(),
		pingOrcDoneFlag: false,
		initC:           make(chan struct{}),
		pingTaskC:       make(chan *models.CidInfo, workers), // TODO: harcoded
		workers:         workers,
	}
}

// Run executes the main logic of the CID Pinger.
// 1. runs the queue logic that schedules the pings
// 2. launchs the pinger pool that will perform all the CID monitoring calls
func (pinger *CidPinger) Run() {
	defer pinger.wg.Done()

	var pingOrchWG sync.WaitGroup
	pingOrchWG.Add(1)
	// Generate the Ping Orchester
	go pinger.generatePingOrchester(&pingOrchWG)

	// Launch CID pingers (workers)
	var pingerWG sync.WaitGroup
	for pingerCounter := 0; pingerCounter < pinger.workers; pingerCounter++ {
		pingerWG.Add(1)
		go pinger.createPinger(&pingerWG, pingerCounter)
	}

	pingOrchWG.Wait()
	log.Infof("finished pinging the CIDs on %d rounds", pinger.rounds)
	close(pinger.pingTaskC)
	pinger.pingOrcDoneFlag = true
	log.Info("closed pingTask channel and set pingOrcDoneFlag to true")
	pingerWG.Wait()
	log.Debug("done from the CID Pinger")
	//close the publisher host
	err := pinger.host.Close()
	if err != nil {
		log.Errorf("failed to close host: %s", err)
		return
	}
}

// AddCidInfo adds a new CID to the pinging queue
func (p *CidPinger) AddCidInfo(c *models.CidInfo) {
	if !p.init {
		p.init = true
		p.initC <- struct{}{}
	}
	p.cidQ.addCid(c)
}

// PingPRHolder dials a given PR Holder to check whether it's active or not, and whether it has the PRs or not
func (pinger *CidPinger) PingPRHolder(c *models.CidInfo, round int, pAddr peer.AddrInfo) *models.PRPingResults {
	logEntry := log.WithFields(log.Fields{
		"cid": c.CID.Hash().B58String(),
	})

	var active, hasRecords bool
	var connError string

	// connect the peer
	pingCtx, cancel := context.WithTimeout(pinger.ctx, DialTimeout)
	defer cancel()

	tstart := time.Now()

	// loop over max tries if the connection is connection refused/ connection reset by peer
	for att := 0; att < maxDialAttempts; att++ {
		// TODO: attempt at least to see if the connection refused
		err := pinger.host.Connect(pingCtx, pAddr)
		if err != nil {
			logEntry.Debugf("unable to connect peer %s for Cid %s - error %s", pAddr.ID.String(), c.CID.Hash().B58String(), err.Error())
			connError = p2p.ParseConError(err)
			// If the error is not linked to an connection refused or reset by peer, just break the look
			if connError != p2p.DialErrorConnectionRefused && connError != p2p.DialErrorStreamReset {
				break
			}
		} else {
			logEntry.Infof("succesful connection to peer %s for Cid %s", pAddr.ID.String(), c.CID.Hash().B58String())
			active = true
			connError = p2p.NoConnError

			// if the connection was successfull, request whether it has the records or not
			provs, _, err := pinger.host.DHT.GetProvidersFromPeer(pinger.ctx, pAddr.ID, c.CID.Hash())
			if err != nil {
				log.Warnf("unable to retrieve providers from peer %s - error: %s", pAddr.ID, err.Error())
			} else {
				logEntry.Debugf("providers for Cid %s from peer %s - %v\n", c.CID.Hash().B58String(), pAddr.ID.String(), provs)
			}

			// iter through the providers to see if it matches with the host's peerID
			for _, paddrs := range provs {
				if paddrs.ID == c.Creator {
					hasRecords = true
					log.Info(time.Now(), "Peer", pAddr.ID.String(), "reporting on", c.CID.Hash().B58String(), " -> ", paddrs)
				} else {
					log.Debugf("Did not find creator for cid %s", c.CID.Hash().B58String())
				}
			}

			// close connection and exit loop
			err = pinger.host.Network().ClosePeer(pAddr.ID)
			if err != nil {
				logEntry.Errorf("unable to close connection to peer %s - %s", pAddr.ID.String(), err.Error())
			}
			break
		}
	}

	fetchTime := time.Since(tstart)

	return models.NewPRPingResults(
		c.CID,
		pAddr.ID,
		round,
		tstart,
		fetchTime,
		active,
		hasRecords,
		connError)
}

//generatePingOrchester handles the way pings are going to happen:
//
//1.) The ping orchester waits for a cid to be generated by:
//	func generateCids(...)
//and the cid to be sent to the:
//	initC chan struct{}
//inside the cid tracker generation by the:
//	func AddCidInfo(...)
//
//2.) Loops over the queue of CidInfos and determines whether a cid needs to be pinged. If the cid needs to be pinged:
//	pinger.pingTaskC <- cidInfo
//TODO connection between this and the next go routine
//
//3.) This is a ongoing step. Keeps a timer using:
//	minTimeT := time.NewTicker(minIterTime)
//and waits until a new iteration occurs to restart looping over the cids:
//	<-minTimeT.C
func (pinger *CidPinger) generatePingOrchester(pingOrchWG *sync.WaitGroup) {
	defer pingOrchWG.Done()

	logEntry := log.WithField("mod", "cid-orchester")
	// we need to wait until the first CID is added, wait otherwise
	// this cid is added inside the AddCidInfo method called from Cid tracker
	<-pinger.initC
	log.Debug("RECEIVED MESSAGE FROM INITC")
	// generate a timer to determine
	minTimeT := time.NewTicker(minIterTime)

	for {
		select {
		case <-pinger.ctx.Done():
			log.Info("shutdown was detected, closing Cid Ping Orchester")
			return

		default:
			// loop over the list of CIDs, and check whether they need to be pinged or not
			for _, cidInfo := range pinger.cidQ.cidArray {
				// check if ctx was was closed
				if pinger.ctx.Err() != nil {
					log.Info("shutdown was detected, closing Cid Ping Orchester")
					return
				}
				cidStr := cidInfo.CID.Hash().B58String()
				// check if the time for next ping has arrived
				//if the time has passed for the next ping
				log.Debugf("CID INFO NEXT PING: %s", cidInfo.NextPing)
				if time.Now().Sub(cidInfo.NextPing) < time.Duration(0) {
					logEntry.Debugf("not in time to ping %s", cidStr)
					//TODO isn't continue better here
					break
				}
				log.Debug("THE TIME TO PING HAS COME")
				// increment Ping Counter
				cidInfo.IncreasePingCounter()

				// Add the CID to the pingTaskC
				pinger.pingTaskC <- cidInfo

				// check if they have reached the max-round counter
				// if yes, remove them from the cidQ.cidArray
				if cidInfo.GetPingCounter() >= pinger.rounds {
					// delete the CID from the list
					pinger.cidQ.removeCid(cidStr)
					logEntry.Infof("finished pinging CID %s - max pings reached %d", cidStr, pinger.rounds)
				}

			}

			// if CID pinger was initialized and there are no more CIDs to track, we are done with the study
			if pinger.cidQ.Len() == 0 {
				return
			}

			// reorganize the array of CIDs from lowest next ping time to biggest one
			pinger.cidQ.sortCidList()

			// check if ticker for next iteration was raised
			<-minTimeT.C
		}
	}
}

//createPinger creates the necessary pinger to retrieve the content from the network and ping the PR holders of the content.
//1.) Receives a cidInfo struct from the channel:
//	pinger.pingTaskC chan *models.CidInfo
//inside the generatePingOrchester goroutine.
//
//2.) Requests the status of the PR holders(substeps):
//
//     1.) Finds the provider of the CIDs
//
//     2.) Recalculates the k closest peers
//The above requests are done to populate the CidFetchRes struct{...} and then
//
//3.) Pings the PR holders in parallel calling the:
//	func PingPRholder(...)
//which adds the:
//	PRPingResults to the CidFetchRes field []*PRPingResults
func (pinger *CidPinger) createPinger(wg *sync.WaitGroup, pingerID int) {
	defer wg.Done()
	minTimeT := time.NewTicker(minIterTime)
	logEntry := log.WithField("pinger", pingerID)
	logEntry.Debug("Initialized")
	for {
		// check if the ping orcherster has finished
		if pinger.pingOrcDoneFlag {
			logEntry.Info("no more pings to orchestrate, finishing worker")
			return
		}
		select {
		case cidInfo, ok := <-pinger.pingTaskC:

			if cidInfo == nil {
				logEntry.Warn("empty CID received from channel, finishing worker")
				return
			}

			if !ok {
				logEntry.Warn("empty CID received from channel, finishing worker")
				return
			}

			// check if the ping orcherster has finished
			if pinger.pingOrcDoneFlag {
				logEntry.Info("no more pings to orchestrate, finishing worker")
				return
			}

			log.Debug("RECEIVED PING TASK FROM CHANNEL")

			cidStr := cidInfo.CID.Hash().B58String()

			pingCounter := cidInfo.GetPingCounter()

			logEntry.Infof("pinging CID %s for round %d", cidStr, pingCounter)

			// request the status of PR Holders
			cidFetchRes := models.NewCidFetchResults(cidInfo.CID, pingCounter)

			var wg sync.WaitGroup

			wg.Add(1)
			// DHT FindProviders call to see if the content is actually retrievable from the network
			go func(p *CidPinger, c *models.CidInfo, fetchRes *models.CidFetchResults) {
				defer wg.Done()
				var isRetrievable bool
				t := time.Now()
				log.Debug("CALLING LOOKUP FOR PROVIDERS")
				providers, err := p.host.DHT.LookupForProviders(p.ctx, c.CID)
				pingTime := time.Since(t)
				fetchRes.FindProvDuration = pingTime
				if err != nil {
					logEntry.Warnf("unable to get the closest peers to cid %s - %s", cidStr, err.Error())
				}
				// iter through the providers to see if it matches with the host's peerID
				for _, paddrs := range providers {
					if paddrs.ID == c.Creator {
						isRetrievable = true
					}
				}
				fetchRes.IsRetrievable = isRetrievable
				log.Debug("FINISHED CALLING LOOKUP FOR PROVIDERS")
			}(pinger, cidInfo, cidFetchRes)

			wg.Add(1)
			// recalculate the closest k peers to the content. This needs to be done due to node churn.(?)
			go func(p *CidPinger, c *models.CidInfo, fetchRes *models.CidFetchResults) {
				defer wg.Done()
				t := time.Now()

				var hops dht.Hops
				log.Debug("CALLING GET CLOSEST PEERS")

				closestPeers, err := p.host.DHT.GetClosestPeers(p.ctx, string(c.CID.Hash()), &hops)
				pingTime := time.Since(t)
				fetchRes.TotalHops = hops.Total
				fetchRes.HopsToClosest = hops.ToClosest
				fetchRes.GetClosePeersDuration = pingTime
				if err != nil {
					logEntry.Warnf("unable to get the closest peers to cid %s - %s", cidStr, err.Error())
				}
				for _, peer := range closestPeers {
					cidFetchRes.AddClosestPeer(peer)
				}
				log.Debug("FINISHED CALLING GET CLOSEST PEERS")
			}(pinger, cidInfo, cidFetchRes)

			// Ping in parallel each of the PRHolders
			for _, peerInfo := range cidInfo.PRHolders {
				wg.Add(1)
				go func(wg *sync.WaitGroup, c *models.CidInfo, peerInfo *models.PeerInfo, fetchRes *models.CidFetchResults) {
					log.Debug("CALLING PING PR HOLDER")
					defer wg.Done()
					pingRes := pinger.PingPRHolder(c, pingCounter, peerInfo.GetAddrInfo())
					fetchRes.AddPRPingResults(pingRes)
					log.Debug("FINISHED CALLING PING PR HOLDER")
				}(&wg, cidInfo, peerInfo, cidFetchRes)
			}

			wg.Wait()

			log.Debug("FINISHED GO ROUTINES")
			// update the finish time for the total fetch round
			cidFetchRes.FinishTime = time.Now()

			// add the fetch results to the array and persist it in the DB
			pinger.dbCli.AddFetchResult(cidFetchRes)
			log.Debug("ADDED FETCH RESULTS TO THE DATABASE")
		case <-pinger.ctx.Done():
			logEntry.Info("shutdown detected, closing pinger")
			return
		default:
			// check if ticker for next iteration was raised
			// check if the ping orcherster has finished
			if pinger.pingOrcDoneFlag {
				logEntry.Info("no more pings to orchestrate, finishing worker")
				return
			}
			<-minTimeT.C
		}
	}
}
