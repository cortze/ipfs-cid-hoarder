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
	"github.com/libp2p/go-libp2p-core/peer"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

//TODO this is hacky and terrible
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
	CidSource CidSource
	CidPinger *CidPinger

	K             int
	CidNumber     int
	Workers       int
	ReqInterval   time.Duration
	StudyDuration time.Duration
	CidMap        sync.Map
}

type CidDiscoverer struct {
	*CidTracker
}

type CidPublisher struct {
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

func NewCidDiscoverer(tracker *CidTracker) (*CidDiscoverer, error) {
	log.Info("Creating a new CID discoverer")
	return &CidDiscoverer{
		CidTracker: tracker,
	}, nil
}

func NewCidPublisher(tracker *CidTracker) (*CidPublisher, error) {
	log.Info("Creating a new CID publisher")
	return &CidPublisher{
		CidTracker: tracker,
	}, nil
}

//TODO this is hacky and terrible
func (tracker *CidTracker) run() {

}

func (publisher *CidPublisher) run() {
	// launch the PRholder reading routine
	msgNotChannel := publisher.MsgNot.GetNotifierChan()

	var firstCidFetchRes sync.Map

	// generate a channel with the same size as the Workers one
	cidChannel := make(chan *cid.Cid, publisher.Workers)

	// IPFS DHT Message Notification Listener
	done := make(chan struct{})
	go publisher.addProviderMsgListener(&firstCidFetchRes, done, msgNotChannel)
	// CID generator
	var genWG sync.WaitGroup
	genWG.Add(1)
	go generateCids(publisher.CidSource, publisher.CidNumber, &genWG, cidChannel)

	var publisherWG sync.WaitGroup

	// CID PR Publishers which are essentially the workers of tracker.
	for publisherCounter := 0; publisherCounter < publisher.Workers; publisherCounter++ {
		publisherWG.Add(1)
		// start the providing process
		go publisher.publishing_process(&publisherWG, publisherCounter, cidChannel, &firstCidFetchRes)
	}
}

func (discoverer *CidDiscoverer) run() {

}

//addProviderMsgListener listens to a:
//	msgNotchannel chan *p2p.MsgNotification
//for a:
//	MsgNotification.Msg.Type of Message_ADD_PROVIDER
//when this message is received it adds the provider onto the database for later pings:
//
//1.)Creates a new:
//	PRPingResults struct{...}
//the result of a ping of an individual PR_HOLDER
//
//2.) Adds the PRPingResults struct{...} into the:
//	var cidFetRes *CidFetchResults
//using:
//	citFetRes.AddPRPingResults(pingRes)
//which is set inside the providing_process routine:
//	fetchRes := models.NewCidFetchResults(*received_cid, 0) // First round = Publish PR
//	cidFetchRes.Store(received_cidStr, fetchRes)
//
//3.)Creates a new:
//	PeerInfo struct{...}
//which is stored inside a cidInfo struct{...}:
//	cidInfo.AddPRHolder(prHolderInfo)
//this is taken by the providing_process routine:
//	cidInfo := models.NewCidInfo(*received_cid, publisher.ReqInterval, config.RandomContent, publisher.CidSource.Type(), publisher.host.ID())
//	// generate the cidFetcher
//	publisher.CidMap.Store(received_cidStr, cidInfo)
func (publisher *CidPublisher) addProviderMsgListener(firstCidFetchRes *sync.Map, done chan struct{}, msgNotChannel chan *p2p.MsgNotification) {
	for {
		select {
		case msgNot := <-msgNotChannel: //this receives a message from SendMessage in messages.go after the DHT.Provide operation is called from the PUT_PROVIDER method.
			if msgNot == nil {
				log.Warn("empty msgNot Received, closing reader for PR Holders")
				return
			}
			casted_cid, err := cid.Cast(msgNot.Msg.GetKey())
			if err != nil {
				log.Error("unable to cast msg key into cid. %s", err.Error())
			}
			switch msgNot.Msg.Type {
			case pb.Message_ADD_PROVIDER:
				var active, hasRecords bool
				var connError string

				if msgNot.Error != nil {
					connError = p2p.ParseConError(msgNot.Error) //TODO: parse the errors in a better way
					log.Debugf("Failed putting PR for CID %s of PRHolder %s - error %s", casted_cid.String(), msgNot.RemotePeer.String(), msgNot.Error.Error())
				} else {
					active = true
					hasRecords = true
					connError = p2p.NoConnError
					log.Debugf("Successfull PRHolder for CID %s of PRHolder %s", casted_cid.String(), msgNot.RemotePeer.String())
				}

				//if no error occured in p2p.MsgNotification the ping result will contain active = true and hasRecords = true,
				//else it will have these fields as false.
				pingRes := models.NewPRPingResults(
					casted_cid,
					msgNot.RemotePeer,
					0, // round is 0 since is the ADD_PROVIDE result
					msgNot.QueryTime,
					msgNot.QueryDuration,
					active,
					hasRecords,
					connError)

				// add the ping result
				val, ok := firstCidFetchRes.Load(casted_cid.Hash().B58String())
				cidFetRes := val.(*models.CidFetchResults)
				if !ok {
					log.Errorf("CidFetcher not ready for cid %s", casted_cid.Hash().B58String())
				} else {
					// TODO: move into a seaparate method to make the DB interaction easier?
					cidFetRes.AddPRPingResults(pingRes)
				}

				useragent := publisher.host.GetUserAgentOfPeer(msgNot.RemotePeer)

				// Generate the new PeerInfo struct for the new PRHolder
				prHolderInfo := models.NewPeerInfo(
					msgNot.RemotePeer,
					publisher.host.Peerstore().Addrs(msgNot.RemotePeer),
					useragent,
				)

				// add PRHolder to the CidInfo
				val, ok = publisher.CidMap.Load(casted_cid.Hash().B58String())
				cidInfo := val.(*models.CidInfo)
				if !ok {
					log.Warnf("unable to find CidInfo on CidMap for Cid %s", casted_cid.Hash().B58String())
				} else {
					cidInfo.AddPRHolder(prHolderInfo)
				}

			default:
				log.Debug("msg is not ADD_PROVIDER msg")
			}

		case <-publisher.ctx.Done():
			log.Info("context has been closed, finishing Cid Tracker")
			return

		case <-done:
			log.Info("all the CIDs have been generated")
			return
		}
	}
}

func (discoverer *CidDiscoverer) foundProviderMsgListener() {

}

//The providing process doesn't only publish the CIDs to the network but it's important for setting up the pinging process used later
//by the tool.
//Things that it does:
//
//1.)Receives the cids from the generateCids(...) method
//
//2.)After receiving a cid it creates a:
//	CIdInfo struct {...} instance
//which documents basic info about a specific CID
//
//3.) Create a:
//	CidFetchResults struct {...}
//which contains basic info about the fetching process (pinging)
//
//4.) Calls the:
//	func provide(...) inside this package cid-tracker.go
//providing the CID to the network
//
//5.) adds the metrics received from func provide(...) and the fetch result struct to the newly created:
//	var cidInfo *CidInfo
//
//6.) Access the tracker's field:
//		DBCli     *db.DBClient
// and adds the cidInfo and the fetchRes
//
//7.) Adds the cid info to the tracker's:
//		CidPinger *CidPinger
//to be later pinged by the pinger.
func (publisher *CidPublisher) publishing_process(publisherWG *sync.WaitGroup, publisherID int, cidChannel chan *cid.Cid, cidFetchRes *sync.Map) {
	defer publisherWG.Done()
	logEntry := log.WithField("publisherID", publisherID)
	ctx := publisher.ctx
	logEntry.Debugf("publisher ready")
	for {
		select {
		case received_cid := <-cidChannel: //this channel receives the CID from the CID generator go routine
			if received_cid == nil {
				logEntry.Warn("received empty CID to track, closing publisher")
				// not needed
				return
			}
			logEntry.Debugf("new cid to publish %s", received_cid.Hash().B58String())
			received_cidStr := received_cid.Hash().B58String()
			// generate the new CidInfo cause a new CID was just received
			//TODO the content type is not necessarily random content
			cidInfo := models.NewCidInfo(*received_cid, publisher.ReqInterval, config.RandomContent, publisher.CidSource.Type(), publisher.host.ID())

			// generate the cidFetcher
			publisher.CidMap.Store(received_cidStr, cidInfo)

			// generate a new CidFetchResults
			fetchRes := models.NewCidFetchResults(*received_cid, 0) // First round = Publish PR
			cidFetchRes.Store(received_cidStr, fetchRes)

			// necessary stuff to get the different hop measurements
			var hops dht.Hops
			// currently linking a ContextKey variable througth the context that we generate
			ctx := context.WithValue(publisher.ctx, dht.ContextKey("hops"), &hops)

			reqTime, err := provide(ctx, publisher, received_cid)
			if err != nil {
				logEntry.Errorf("unable to Provide content. %s", err.Error())
			}
			// add the number of hops to the fetch results
			fetchRes.TotalHops = hops.Total
			fetchRes.HopsToClosest = hops.ToClosest

			// TODO: fix this little wait to comput last PR Holder status
			// little not inside the CID to notify when k peers where recorded?
			time.Sleep(500 * time.Millisecond)

			// add the request time to the CidInfo
			cidInfo.AddProvideTime(reqTime)
			cidInfo.AddPRFetchResults(fetchRes)

			// the Cid has already being published to the network, we can already save it into the DB
			// ----- Persist inot DB -------
			// Add the cidInfo to the DB
			publisher.DBCli.AddCidInfo(cidInfo)

			// Add the fetchResults to the DB
			publisher.DBCli.AddFetchResult(fetchRes)
			// ----- End of the persist into DB -------

			// Calculate success ratio on adding PR into PRHolders
			tot, success, failed := cidInfo.GetFetchResultSummaryOfRound(0)
			if tot < 0 {
				logEntry.Warnf("no ping results for the PR provide round of Cid %s", cidInfo.CID.Hash().B58String())
			} else {
				logEntry.Infof("Cid %s - %d total PRHolders | %d successfull PRHolders | %d failed PRHolders",
					received_cid, tot, success, failed)
			}

			// send the cid_info to the cid_pinger adn start pinging PR Holders
			publisher.CidPinger.AddCidInfo(cidInfo)

		case <-ctx.Done():
			logEntry.WithField("publisherID", publisherID).Debugf("shutdown detected, closing publisher")
			return
		}
	}
}

func (discoverer *CidDiscoverer) discovery_process() {

}

func generateCids(source CidSource, cidNumber int, wg *sync.WaitGroup, cidChannel chan *cid.Cid) {
	defer wg.Done()
	// generate the CIDs
	for i := 0; i < cidNumber; i++ {
		_, contID, err := source.GetNewCid()
		if err != nil {
			log.Errorf("unable to generate %s content. %s", err.Error(), source.Type())
		}
		cidChannel <- &contID
	}
}

//provide calls:
//	DHT.Provide(...) method to provide the cid to the network
// and documents the time it took to publish a CID
func provide(ctx context.Context, publisher *CidPublisher, receivedCid *cid.Cid) (time.Duration, error) {
	tstart := time.Now()
	err := publisher.host.DHT.Provide(publisher.ctx, *receivedCid, true)
	if err != nil {
		return -1, err
	}
	reqTime := time.Since(tstart)
	return reqTime, nil
}

//discover calls:
//	DHT.FindProviders(...) method to discover the providers for the given CID
//also documents the discovery time
func discover(ctx context.Context, discoverer *CidDiscoverer, receivedCid *cid.Cid) (time.Duration, []peer.AddrInfo, error) {
	tstart := time.Now()
	peers, err := discoverer.host.DHT.FindProviders(discoverer.ctx, *receivedCid)
	if err != nil {
		return -1, nil, err
	}
	discoverTime := time.Since(tstart)
	return discoverTime, peers, nil
}
