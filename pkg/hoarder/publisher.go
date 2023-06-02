package hoarder

import (
	"context"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	"go.uber.org/atomic"

	"github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	maxPublicationTime = 2 * time.Minute
)

type CidPublisher struct {
	ctx   context.Context
	appWG *sync.WaitGroup

	host         *p2p.DHTHost
	dhtProvide   p2p.ProvideOption
	DBCli        *db.DBClient
	cidGenerator *CidGenerator

	//receive message from listen for add provider message function
	MsgNot      *p2p.MsgNotifier
	K           int
	Workers     int
	ReqInterval time.Duration
	CidPingTime time.Duration

	// main set of Cids that will keep track of them over the run
	cidSet         *cidSet
	metrics        *publisherMetrics
	generationDone *atomic.Bool
}

func NewCidPublisher(
	ctx context.Context,
	appWG *sync.WaitGroup,
	hostOpts p2p.DHTHostOptions,
	db *db.DBClient,
	generator *CidGenerator,
	cidSet *cidSet,
	k, workers int,
	reqInterval,
	cidPingTime time.Duration,
) (*CidPublisher, error) {

	log.WithField("mod", "publisher").Info("initializing...")
	h, err := p2p.NewDHTHost( // host is already bootstrapped
		ctx,
		hostOpts,
	)
	if err != nil {
		return nil, errors.Wrap(err, "publisher:")
	}
	log.WithField("mod", "publisher").Info("initialized...")
	return &CidPublisher{
		ctx:            ctx,
		appWG:          appWG,
		host:           h,
		dhtProvide:     hostOpts.ProvOp,
		DBCli:          db,
		MsgNot:         h.GetMsgNotifier(),
		cidGenerator:   generator,
		K:              k,
		ReqInterval:    reqInterval,
		CidPingTime:    cidPingTime,
		Workers:        workers,
		cidSet:         cidSet,
		metrics:        newPublisherMetrics(string(hostOpts.ProvOp)),
		generationDone: atomic.NewBool(false),
	}, nil
}

func (publisher *CidPublisher) Run() {
	defer publisher.appWG.Done()
	// launch the PRholder reading routine
	msgNotChannel := publisher.MsgNot.GetNotifierChan()

	// control variables
	var publisherWG sync.WaitGroup
	var msgNotWG sync.WaitGroup
	var firstCidFetchRes sync.Map
	genDoneCs := make([]chan struct{}, 0, publisher.Workers) // one per each publisher instance
	publicationDoneC := make(chan struct{})                  // there is one singel msg-not-reader

	plog := log.WithField("mod", "publisher")
	// IPFS DHT Message Notification Listener
	msgNotWG.Add(1)
	go publisher.addProviderMsgListener(
		&msgNotWG,
		publicationDoneC,
		&firstCidFetchRes,
		msgNotChannel,
	)

	cidPubC, genWG := publisher.cidGenerator.Run()
	for publisherCounter := 0; publisherCounter < publisher.Workers; publisherCounter++ {
		publisherWG.Add(1)
		generationDoneC := make(chan struct{})
		go publisher.publishingProcess(
			&publisherWG,
			generationDoneC,
			publisherCounter,
			cidPubC,
			&firstCidFetchRes,
		)
		genDoneCs = append(genDoneCs, generationDoneC)
	}

	genWG.Wait()
	plog.Info("generation process finished successfully")
	publisher.generationDone.Swap(true)
	// notify each of the publishers that the generation has finished
	for _, generationDoneC := range genDoneCs {
		generationDoneC <- struct{}{}
		close(generationDoneC)
	}

	publisherWG.Wait()
	plog.Info("publication process finished successfully")
	publicationDoneC <- struct{}{}

	msgNotWG.Wait()
	plog.Info("msg notification channel finished successfully")

	publisher.host.Close()
	plog.Info("publisher successfully closed")
	close(publicationDoneC)
}

// addProviderMsgListener listens the Notchannel for ADD_PROVIDER messages for the provided CIDs
// from each of the messages received, it composes/adds a new PR holder to the CID
// finally, it aggregates all the PingRound info of the publication as the first PingRound (0)
func (publisher *CidPublisher) addProviderMsgListener(
	msgNotWg *sync.WaitGroup,
	publicationDoneC chan struct{},
	firstCidFetchRes *sync.Map,
	msgNotChannel chan *p2p.MsgNotification) {
	defer func() {
		// notify that the msg listener has been closed
		msgNotWg.Done()
	}()
	mlog := log.WithField("service", "msg-listener")
	for {
		select {
		// this receives a message from SendMessage in messages.go after the DHT.Provide operation
		// is called from the PUT_PROVIDER method.
		case msgNot := <-msgNotChannel:
			// check the msg type
			castedCid, err := cid.Cast(msgNot.Msg.GetKey())
			if err != nil {
				mlog.Errorf("unable to cast msg key into cid. %s", err.Error())
			}
			switch msgNot.Msg.Type {
			case pb.Message_ADD_PROVIDER:
				var active bool
				var connError string

				if msgNot.Error != nil {
					//TODO: parse the errors in a better way
					connError = p2p.ParseConError(msgNot.Error)
					mlog.Debugf("Failed putting PR for CID %s of PRHolder %s - error %s",
						castedCid.Hash().B58String(), msgNot.RemotePeer.String(), msgNot.Error.Error(),
					)
				} else {
					// assume that if the peer replies successfully to the ADD_PROVIDER messages,
					// the remote peer keeps the info (We are assuming also this for the Hydras)
					active = true
					connError = p2p.NoConnError
					mlog.Debugf("Successfull PRHolder for CID %s of PRHolder %s", castedCid.Hash().B58String(), msgNot.RemotePeer.String())
				}

				// Read the CidInfo from the local Sync.Map struct
				cidInfo, ok := publisher.cidSet.getCid(castedCid.Hash().B58String())
				if !ok {
					mlog.Panic("unable to find CidInfo on CidSet for Cid ", castedCid.Hash().B58String())
				}

				// add the ping result
				val, ok := firstCidFetchRes.Load(castedCid.Hash().B58String())
				cidFetRes := val.(*models.CidFetchResults)
				if !ok {
					mlog.Panicf("CidFetcher not found for cid %s", castedCid.Hash().B58String())
				}

				// save the ping result into the FetchRes
				cidFetRes.AddPRPingResults(models.NewPRPingResults(
					castedCid,
					msgNot.RemotePeer,
					0, // round is 0 since is the ADD_PROVIDE result
					cidFetRes.GetPublicationTime(),
					msgNot.QueryTime,
					msgNot.QueryDuration,
					active,
					false,
					false,
					connError),
				)

				// Generate the new PeerInfo struct for the new PRHolder
				prHolderInfo := models.NewPeerInfo(
					msgNot.RemotePeer,
					publisher.host.GetMAddrsOfPeer(msgNot.RemotePeer),
					publisher.host.GetUserAgentOfPeer(msgNot.RemotePeer),
				)

				// add all the PRHolder info to the CidInfo
				cidInfo.AddPRHolder(prHolderInfo)

				if cidFetRes.IsDone() {
					// notify the publisher that the CID is ready
					cidFetRes.DoneC <- struct{}{}
				}

			default:
				// the message that we tracked is not ADD_PROVIDER, skipping
			}

		case <-publisher.ctx.Done():
			log.Info("context has been closed, finishing Cid Publisher")
			return

		case <-publicationDoneC:
			// if the publication has finished, we don't expect more messages to arrive, the host
			// has been shut down
			log.Info("publication done and not missing sent msgs to check, closing msgNotChannel")
			return
		}
	}
}

// publisherService is a service that generates random CID from the generator
// and publishes them to the IPFS network based on the specified configuration
// the publisher also tracks the provide operation instrumenting it, persisting the metadata
// into the DB, and adding the CID to the pingerService
func (publisher *CidPublisher) publishingProcess(
	publisherWG *sync.WaitGroup,
	generationDoneC chan struct{},
	publisherID int,
	cidChannel chan *cid.Cid,
	cidFetchRes *sync.Map) {

	defer publisherWG.Done()

	// if there is no msg to check and ctx is still active, check if we have finished
	plog := log.WithField("publisher-id", publisherID)
	plog.Debugf("publisher ready")

	generationDone := false
	minIterTicker := time.NewTicker(minIterTime)

	for {
		// check if the generation is done to finish the publisher (with priority)
		if generationDone && len(cidChannel) == 0 {
			plog.Info("no cid is waiting to be published, closing")
			return
		}
		select {
		case nextCid := <-cidChannel: //this channel receives the CID from the CID generator go routine
			cidStr := nextCid.Hash().B58String()
			plog.Debugf("new cid to publish %s", cidStr)

			pCtx, cancel := context.WithTimeout(publisher.ctx, maxPublicationTime)

			// generate the new CidInfo cause a new CID was just received
			cidInfo := models.NewCidInfo(
				*nextCid,
				publisher.K,
				publisher.ReqInterval,
				publisher.CidPingTime,
				string(publisher.dhtProvide),
				publisher.host.ID(),
			)

			// track the new Cid into the cidSet
			publisher.cidSet.addCid(cidInfo)
			pubTime := time.Now()
			// compose the fetchRes of the publication phase
			fetchRes := models.NewCidFetchResults(*nextCid, pubTime, 0, publisher.K)
			cidFetchRes.Store(cidStr, fetchRes)

			reqTime, lookupMetrics, err := publisher.host.ProvideCid(pCtx, cidInfo)
			if err != nil {
				plog.Errorf("unable to Provide content. %s", err.Error())
			}
			if lookupMetrics != nil {
				fetchRes.TotalHops = lookupMetrics.GetTotalHops()
				fetchRes.HopsTreeDepth = lookupMetrics.GetTreeDepth()
				fetchRes.MinHopsToClosest = lookupMetrics.GetMinHopsForPeerSet(lookupMetrics.GetClosestPeers())
			} else {
				fetchRes.TotalHops = -1
				fetchRes.HopsTreeDepth = -1
				fetchRes.MinHopsToClosest = -1
			}

			// Make sure we have received all the messages from the publication
			// but do not wait forever
			select {
			case <-fetchRes.DoneC:
				plog.Trace("finished publishing cid")
			case <-pCtx.Done():
				plog.Warnf("timeout publishing CID reached")
				// give 500 ms extra to see if we receive any not from the msg-notifier
				time.Sleep(500 * time.Millisecond)
			}
			cancel()

			// update the info of the Cid After its publication
			cidInfo.AddPublicationTime(pubTime)
			cidInfo.AddProvideTime(reqTime)
			cidInfo.AddPRFetchResults(fetchRes)

			// add to the metrics
			publisher.metrics.addCid(string(publisher.dhtProvide))

			// the Cid has already being published, save it into the DB
			publisher.DBCli.AddCidInfo(cidInfo)
			publisher.DBCli.AddFetchResult(fetchRes)

			// print summary of the publication (round 0)
			publisher.printSummary(plog, cidInfo, 0)

		case <-publisher.ctx.Done():
			plog.Debugf("shutdown detected, closing publisher")
			return

		case <-generationDoneC:
			plog.Debug("generation done detected")
			generationDone = true

		case <-minIterTicker.C:
			// keep checking if the generation has ended to close the routine
		}
		minIterTicker.Reset(minIterTime)
	}
}

// printSummary shows in the stdout the publication summary of a given CID
func (publisher *CidPublisher) printSummary(logE *log.Entry, cInfo *models.CidInfo, round int) {
	// Calculate success ratio on adding PR into PRHolders
	tot, success, failed := cInfo.GetFetchResultSummaryOfRound(round)
	if tot < 0 {
		logE.Warnf("no ping results for the PR provide round of Cid %s",
			cInfo.CID.Hash().B58String())
	} else {
		logE.Infof("Cid %s - %d total PRHolders | %d successfull PRHolders | %d failed PRHolders",
			cInfo.CID.Hash().B58String(), tot, success, failed)
	}
}

func (publisher *CidPublisher) Close() {
	// close the generator and everything else will be closed in cascade
	if !publisher.generationDone.Load() {
		publisher.cidGenerator.Close()
	}
}

func (publisher *CidPublisher) GetTotalPublishedCids() map[string]uint64 {
	provOpCids := publisher.metrics.getCidPublicationNumbers()
	return provOpCids
}

type publisherMetrics struct {
	m sync.RWMutex

	counter map[string]uint64
}

func newPublisherMetrics(provOps ...string) *publisherMetrics {
	counter := make(map[string]uint64)
	for _, provOp := range provOps {
		counter[provOp] = 0
	}
	return &publisherMetrics{
		counter: counter,
	}
}

func (m *publisherMetrics) addCid(provOp string) uint64 {
	m.m.Lock()
	defer m.m.Unlock()
	cnt, ok := m.counter[provOp]
	if !ok {
		m.counter[provOp] = 0
	}
	m.counter[provOp] = cnt + 1
	return cnt + 1
}

func (m *publisherMetrics) getCidPublicationNumbers() map[string]uint64 {
	m.m.RLock()
	defer m.m.RUnlock()
	cntCopy := make(map[string]uint64)

	for op, val := range m.counter {
		cntCopy[op] = val
	}
	return cntCopy
}
