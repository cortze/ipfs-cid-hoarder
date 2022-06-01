package hoarder

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	log "github.com/sirupsen/logrus"

	"github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

type CidSource interface {
	GetNewCid() ([]byte, cid.Cid, error)
	Type() string
}

type CidTracker struct {
	ctx context.Context

	m sync.Mutex

	host      *p2p.Host
	DBCli     *db.DBClient
	MsgNot    *p2p.Notifier
	CidSource CidSource

	K             int
	CidNumber     int
	BatchSize     int
	ReqInterval   time.Duration
	StudyDuration time.Duration
	CidMap        sync.Map
	CidFetcherMap sync.Map
}

func NewCidTracker(ctx context.Context, h *p2p.Host, db *db.DBClient, cidSource CidSource, k, cidNum, batchSize int, reqInterval, studyDuration string) (*CidTracker, error) {
	reqInt, err := time.ParseDuration(reqInterval)
	if err != nil {
		return nil, err
	}
	fmt.Println(studyDuration)
	studyDur, err := time.ParseDuration(studyDuration)
	if err != nil {
		return nil, err
	}
	return &CidTracker{
		ctx:           ctx,
		host:          h,
		DBCli:         db,
		MsgNot:        h.GetMsgNotifier(),
		CidSource:     cidSource,
		K:             k,
		CidNumber:     cidNum,
		ReqInterval:   reqInt,
		StudyDuration: studyDur,
		BatchSize:     batchSize,
	}, nil
}

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

func (t *CidTracker) newRandomCidTracker() {
	log.Info("launching the Random Cid Tracker")

	// launch the PRholder reading routine
	msgNotC := t.MsgNot.GetNotChan()

	// generate a channel with the same size as the batchsize one
	cidC := make(chan *cid.Cid, t.BatchSize)

	var firstCidFetchRes sync.Map

	// IPFS DHT Message Notification Listener
	done := make(chan struct{})
	go func() {
		defer func() {
			close(msgNotC)
		}()

		for {
			select {
			case msgNot := <-msgNotC:
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
				log.Info("all the CIDs have been requested")
				return
			}
		}
	}()

	closeWorkersC := make(chan struct{})
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
		// inform workers that all the CIDs have been

	}(t, &genWG, cidC)

	var workerWG sync.WaitGroup

	// CID PR Publishers
	for worker := 0; worker < 25; worker++ {
		workerWG.Add(1)
		// launch worker
		go func(ctx context.Context, wg *sync.WaitGroup, workerID int, cidC chan *cid.Cid, cidFetchRes *sync.Map, done *chan struct{}) {
			defer wg.Done()
			logEntry := log.WithField("workerID", workerID)
			logEntry.Debugf("worker ready")
			for {
				select {
				case c := <-cidC:
					logEntry.Debugf("new cid to publish %s", c.Hash().B58String())
					cStr := c.Hash().B58String()
					// generate the new CidInfo
					cidInfo := models.NewCidInfo(*c, time.Duration(t.ReqInterval)*time.Minute, config.RandomContent, config.RandomSource, t.host.ID())

					// generate the cidFecher
					t.CidMap.Store(cStr, cidInfo)

					// generate a new CidFetchResults
					fetchRes := models.NewCidFetchResults(*c, 0) // First round = Publish PR
					cidFetchRes.Store(cStr, fetchRes)

					tstart := time.Now()
					err := t.host.DHT.Provide(t.ctx, *c, true)
					if err != nil {
						logEntry.Errorf("unable to Provide random content. %s", err.Error())
					}
					reqTime := time.Since(tstart)

					// the Cid has already being published to the network, we can already save it into the DB

					// add the request time to the CidInfo
					cidInfo.AddReqTimeInterval(reqTime)
					cidInfo.AddPRFetchResults(fetchRes)

					// TODO: generate a new DB saving interface to speed up the CID providing process
					// ----- Persist inot DB -------
					// Add the cidInfo to the DB
					err = t.DBCli.AddNewCidInfo(cidInfo)
					if err != nil {
						logEntry.Fatalln("unable to persist to DB new cid info", err)
					}
					// loop over the PRHoders
					for _, prHolder := range cidInfo.PRHolders {
						err = t.DBCli.AddNewPeerInfo(&cidInfo.CID, prHolder)
					}
					// Add the fetchResults to the DB
					err = t.DBCli.AddFetchResults(fetchRes)
					if err != nil {
						logEntry.Fatalln("unable to persist to DB new fetch_results", err)
					}
					err = t.DBCli.AddPingResultsSet(fetchRes.PRPingResults)
					if err != nil {
						logEntry.Fatalln("unable to persist to DB new ping_results", err)
					}
					// ----- End of the persist into DB -------

					// Calculate success ratio on adding PR into PRHolders
					tot, success, failed := cidInfo.GetFetchResultSummaryOfRound(0)
					if tot < 0 {
						logEntry.Warnf("no ping results for the PR provide round of Cid %s", cidInfo.CID.Hash().B58String())
					} else {
						logEntry.Infof("Cid %s - %d total PRHolders | %d successfull PRHolders | %d failed PRHolders",
							c, tot, success, failed)
					}

					// TODO: send the cid_info to the cid_fetcher adn start pinging PR Holders

				case <-*done:
					logEntry.WithField("workerID", workerID).Debugf("shutdown detected, closing worker")
					return

				case <-ctx.Done():
					logEntry.WithField("workerID", workerID).Debugf("shutdown detected, closing worker")
					return
				}
			}

		}(t.ctx, &workerWG, worker, cidC, &firstCidFetchRes, &closeWorkersC)
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
	// order workers to close
	closeWorkersC <- struct{}{}
	close(closeWorkersC)

	// close Msg Notifier
	done <- struct{}{}
	close(msgNotC)
	close(done)

	// wait untill all the workers finished generating the CIDs
	workerWG.Wait()

	// close the

}

func (t *CidTracker) trackRandomCids() {
	// general WaitGroup for all the Fetchers
	var fetchWg sync.WaitGroup

	done := make(chan struct{})
	log.Info("launching the Random Cid Tracker")

	// launch the PRholder reading routine
	msgNotC := t.MsgNot.GetNotChan()

	// generate first round of CidFetch results (to keep track of which peers got the PR)
	var firstCidFetchRes sync.Map

	// TODO: make here the batching of the CID (making the following code a method of the CidTracker)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer func() {
			close(msgNotC)
			wg.Done()
		}()

		for {
			select {
			case msgNot := <-msgNotC:
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
				log.Info("all the CIDs have been requested")
				return
			}
		}
	}()

	// calculate and launch the batches
	totBatches := t.CidNumber / t.BatchSize
	rem := t.CidNumber % t.BatchSize
	if rem > 0 {
		totBatches++
	}

	// hardcoded the number of rounds that we want to ping the PR from holder peers
	rounds := int(t.StudyDuration/t.ReqInterval) + 1

	batchD := t.ReqInterval / time.Duration(totBatches)

	var batchDelay time.Duration
	// get the time that we have to wait between batches for the next round
	log.Infof("with the given number of CIDs (%d), the batch size (%d), and req interval (%s), the delay between batches is %s", t.CidNumber, t.BatchSize, t.ReqInterval, batchD)
	if (batchD) == 0 {
		// since we are using ints, if the delay is 0 we divide the
		batchDelay = t.ReqInterval / time.Duration(2)
	} else {
		batchDelay = batchD
		// gen new timer for the given time
	}

	fmt.Println("batch delay ticker", batchDelay)
	batchDelayTicker := time.NewTicker(batchDelay)
	totCid := 1

	log.Infof("generating %d random Cids", t.CidNumber)
	for batch := 1; batch <= totBatches; batch++ {
		//
		var batchCids sync.Map
		var cidGenWg sync.WaitGroup

		for i := totCid; i <= t.CidNumber && i <= (t.BatchSize*batch); i++ {
			// TODO: parallelize this for performance improvement
			cidGenWg.Add(1)
			go func(genWg *sync.WaitGroup, t *CidTracker, cidFetchRes *sync.Map) { // TODO: might bring up race conditions accessing the CidMap and the FetcherMap
				defer cidGenWg.Done()
				_, contID, err := t.CidSource.GetNewCid()
				if err != nil {
					log.Errorf("unable to generate random content. %s", err.Error())
				}

				// generate the new CidInfo
				cidInfo := models.NewCidInfo(contID, time.Duration(t.ReqInterval)*time.Minute, config.RandomContent, config.RandomSource, t.host.ID())

				// generate the cidFecher
				t.CidMap.Store(contID.Hash().B58String(), cidInfo)
				batchCids.Store(contID.Hash().B58String(), cidInfo)

				// generate a new CidFetchResults
				fetchRes := models.NewCidFetchResults(contID, 0) // first round of the
				cidFetchRes.Store(contID.Hash().B58String(), fetchRes)

				// TODO: harcoded the 5 iterations
				cidFetcher := NewCidFetcher(t.ctx, &fetchWg, t.host, t.DBCli, cidInfo, t.ReqInterval, rounds)
				t.CidFetcherMap.Store(contID.Hash().B58String(), cidFetcher)

				tstart := time.Now()
				err = t.host.DHT.Provide(t.ctx, contID, true)
				if err != nil {
					log.Errorf("unable to Provide random content. %s", err.Error())
				}
				reqTime := time.Since(tstart)

				// add the request time to the CidInfo
				cidInfo.AddReqTimeInterval(reqTime)
			}(&cidGenWg, t, &firstCidFetchRes)

			totCid++
		}
		// wait untill all the cid have been generated
		cidGenWg.Wait()

		// hardfix to wait untill all the PR Add msgs have been successfully added to the firstCidFetchRes
		// TODO: add the K number to the CidInfo, so we can check when we have recorded the k ping results for each of the fetch rounds
		time.Sleep(1 * time.Second)

		// Important - Add the first round of PRHolder pings into the CidInfo
		firstCidFetchRes.Range(func(key, value interface{}) bool {
			c := key.(string)
			cidFetchRes := value.(*models.CidFetchResults)

			// add the first round of FetchResults to the CidMap
			val, ok := t.CidMap.Load(c)
			cidInfo := val.(*models.CidInfo)
			if !ok {
				log.Warnf("no Cid %s on CidMap", c)
			}

			// ----- Persist inot DB -------
			// TODO: add this to a CidTracker method (to make the persisting to DB easier)
			// Add the cidInfo to the DB
			err := t.DBCli.AddNewCidInfo(cidInfo)
			if err != nil {
				log.Fatalln("unable to persist to DB new cid info", err)
			}
			// loop over the PRHoders
			for _, prHolder := range cidInfo.PRHolders {
				err = t.DBCli.AddNewPeerInfo(&cidInfo.CID, prHolder)
			}

			// TODO: add this to a CidTracker method (to make the persisting to DB easier)
			cidInfo.AddPRFetchResults(cidFetchRes)
			// Add the fetchResults to the DB
			err = t.DBCli.AddFetchResults(cidFetchRes)
			if err != nil {
				log.Fatalln("unable to persist to DB new fetch_results", err)
			}
			err = t.DBCli.AddPingResultsSet(cidFetchRes.PRPingResults)
			if err != nil {
				log.Fatalln("unable to persist to DB new ping_results", err)
			}
			// ----- End of the persist into DB -------

			// reset the firstCidFetchRes map for the next batch
			firstCidFetchRes.Delete(c)
			return true
		})

		// generate the CIDFetchers for each of the CIDs
		batchCids.Range(func(key, value interface{}) bool {
			c := key.(string)
			cidInfo := value.(*models.CidInfo)

			// get the first PRPingResults
			if cidInfo.GetFetchResultsLen() == 0 {
				log.Warnf("unable to find CidInfo for Cid %s", c)
				return true
			}
			// Calculate success ratio on adding PR into PRHolders
			tot, success, failed := cidInfo.GetFetchResultSummaryOfRound(0)
			if tot < 0 {
				log.Warnf("no ping results for the PR provide round of Cid %s", cidInfo.CID.Hash().B58String())
			} else {
				log.Infof("Cid %s - %d total PRHolders | %d successfull PRHolders | %d failed PRHolders",
					c, tot, success, failed)
			}

			// check if we managed to put the records in the
			if success > 0 {
				cidInfo.PRPingResults[0].IsRetrievable = true
			}

			// create and launch the fetcher for the given CID
			val, ok := t.CidFetcherMap.Load(cidInfo.CID.Hash().B58String())
			fetcher := val.(*CidFetcher)
			if !ok {
				log.Warnf("no fetcher for the Cid %s was initialized", cidInfo.CID.Hash().B58String())
			}

			fetchWg.Add(1)
			go fetcher.Run()

			return true
		})

		log.Infof("batch %d of %d Cids done!", batch, t.BatchSize)
		// wait untill the ticker raises to launch the next batch
		<-batchDelayTicker.C
	}
	// send signal to previous go routine saying that the cid generatio process has finished
	done <- struct{}{}
	close(done)

	// close the ticker
	batchDelayTicker.Stop() // TODO: Should I close the channel by myself?

	// wait until all the PRholders has been received / or untill the go routine has been shutted down
	wg.Wait()

	log.Infof("finished publishing the %d Cids on %d batches", totCid-1, totBatches)

	fetchWg.Wait()

}
