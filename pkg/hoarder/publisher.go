package hoarder

import (
	"context"
	"sync"
	"time"

	src "github.com/cortze/ipfs-cid-hoarder/pkg/cid-source"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	log "github.com/sirupsen/logrus"
)

type CidPublisher struct {
	//entries of this map are added inside the publishingProccess and received from addProviderMessageListener
	CidMap sync.Map
	//number of cids to publish
	CidNumber int64
	*CidTracker
}

func NewCidPublisher(tracker *CidTracker, h *p2p.Host, cidNum int64) (*CidPublisher, error) {
	log.Debug("Creating a new CID publisher")
	return &CidPublisher{
		CidNumber:  cidNum,
		CidTracker: tracker,
	}, nil
}

func (publisher *CidPublisher) run() {
	defer publisher.wg.Done()
	// launch the PRholder reading routine
	msgNotChannel := publisher.MsgNot.GetNotifierChan()

	// control variables
	var firstCidFetchRes sync.Map

	var genWG sync.WaitGroup
	var publisherWG sync.WaitGroup
	var msgNotWG sync.WaitGroup
	var generationDone bool = false
	var publicationDone bool = false

	// generate a channel with the same size as the Workers one
	trackableCidC := make(chan *src.TrackableCid, publisher.Workers)

	// IPFS DHT Message Notification Listener
	msgNotWG.Add(1)
	go publisher.addProviderMsgListener(&msgNotWG, &publicationDone, &firstCidFetchRes, msgNotChannel)

	// CID generator
	genWG.Add(1)
	go publisher.generateCids(&genWG, trackableCidC)

	// CID PR Publishers which are essentially the workers of tracker.
	for publisherCounter := 0; publisherCounter < publisher.Workers; publisherCounter++ {
		publisherWG.Add(1)
		// start the providing process
		go publisher.publishingProcess(&publisherWG, &generationDone, publisherCounter, trackableCidC, &firstCidFetchRes)
	}

	// wait until the generation of the CIDs is done
	genWG.Wait()
	generationDone = true
	log.Info("generation process finished successfully")

	// wait until the publication of the CIDs is done
	publisherWG.Wait()
	publicationDone = true
	log.Info("publication process finished successfully")

	// wait until the msg not reader has ended
	msgNotWG.Wait()
	log.Info("msg notification channel finished successfully")

	//close the publisher host
	err := publisher.host.Close()
	if err != nil {
		log.Errorf("failed to close publisher host: %s", err)
		return
	}
}

// addProviderMsgListener listens to a:
//
//	msgNotchannel chan *p2p.MsgNotification
//
// for a:
//
//	MsgNotification.Msg.Type of Message_ADD_PROVIDER
//
// when this message is received it adds the provider onto the database for later pings:
//
// 1.)Creates a new:
//
//	PRPingResults struct{...}
//
// the result of a ping of an individual PR_HOLDER
//
// 2.) Adds the PRPingResults struct{...} into the:
//
//	var cidFetRes *CidFetchResults
//
// using:
//
//	citFetRes.AddPRPingResults(pingRes)
//
// which is set inside the providing_process routine:
//
//	fetchRes := models.NewCidFetchResults(*received_cid, 0) // First round = Publish PR
//	cidFetchRes.Store(received_cidStr, fetchRes)
//
// 3.)Creates a new:
//
//	PeerInfo struct{...}
//
// which is stored inside a cidInfo struct{...}:
//
//	cidInfo.AddPRHolder(prHolderInfo)
//
// this is taken by the providing_process routine:
//
//	cidInfo := models.NewCidInfo(*received_cid, publisher.ReqInterval, config.RandomContent, publisher.CidSource.Type(), publisher.host.ID())
//	// generate the cidFetcher
//	publisher.CidMap.Store(received_cidStr, cidInfo)
func (publisher *CidPublisher) addProviderMsgListener(msgNotWg *sync.WaitGroup, publicationDone *bool, firstCidFetchRes *sync.Map, msgNotChannel <-chan *p2p.MsgNotification) {
	defer func() {
		// notify that the msg listener has been closed
		msgNotWg.Done()
	}()
	for {
		// check if we are done with the publication (with priority)
		if *publicationDone && (len(msgNotChannel) == 0) {
			log.Info("publication done and not missing sent msgs to check, closing msgNotChannel")
			return
		}

		select {
		case msgNot := <-msgNotChannel: //this receives a message from SendMessage in messages.go after the DHT.Provide operation is called from the PUT_PROVIDER method.
			// check the msg type
			castedCid, err := cid.Cast(msgNot.Msg.GetKey())
			if err != nil {
				log.Errorf("unable to cast msg key into cid. %s", err.Error())
			}
			switch msgNot.Msg.Type {
			case pb.Message_ADD_PROVIDER:
				var active, hasRecords bool
				var connError string

				if msgNot.Error != nil {
					connError = p2p.ParseConError(msgNot.Error) //TODO: parse the errors in a better way
					log.Debugf("Failed putting PR for CID %s of PRHolder %s - error %s", castedCid.String(), msgNot.RemotePeer.String(), msgNot.Error.Error())
				} else {
					active = true
					hasRecords = true
					connError = p2p.NoConnError
					log.Debugf("Successfull PRHolder for CID %s of PRHolder %s", castedCid.String(), msgNot.RemotePeer.String())
				}

				// Read the CidInfo from the local Sync.Map struct
				val, ok := publisher.CidMap.Load(castedCid.Hash().B58String())
				cidInfo := val.(*models.CidInfo)
				if !ok {
					log.Panic("unable to find CidInfo on CidMap for Cid %s", castedCid.Hash().B58String())
				}

				// add the ping result
				val, ok = firstCidFetchRes.Load(castedCid.Hash().B58String())
				cidFetRes := val.(*models.CidFetchResults)
				if !ok {
					log.Panicf("CidFetcher not found for cid %s", castedCid.Hash().B58String())
				}

				//if no error occurred in p2p.MsgNotification the ping result will contain active = true and hasRecords = true,
				//else it will have these fields as false.
				pingRes := models.NewPRPingResults(
					castedCid,
					msgNot.RemotePeer,
					0, // round is 0 since is the ADD_PROVIDE result
					cidFetRes.GetPublicationTime(),
					msgNot.QueryTime,
					msgNot.QueryDuration,
					active,
					hasRecords,
					connError)

				// TODO: move into a seaparate method to make the DB interaction easier?
				cidFetRes.AddPRPingResults(pingRes)

				useragent := publisher.host.GetUserAgentOfPeer(msgNot.RemotePeer)
				log.Debugf("User agent is: %s", useragent)

				// Generate the new PeerInfo struct for the new PRHolder
				prHolderInfo := models.NewPeerInfo(
					msgNot.RemotePeer,
					publisher.host.Peerstore().Addrs(msgNot.RemotePeer),
					useragent,
				)

				// add all the PRHolder info to the CidInfo
				cidInfo.AddPRHolder(prHolderInfo)

			default:
				// the message that we tracked is not ADD_PROVIDER, skipping
			}

		case <-publisher.ctx.Done():
			log.Info("context has been closed, finishing Cid Tracker")
			return
		default:
			// if there is no msg to check and ctx is still active, check if we have finished
		}
	}
}

// The providing process doesn't only publish the CIDs to the network but it's important for setting up the pinging process used later
// by the tool.
// Things that it does:
//
// 1.)Receives the cids from the generateCids(...) method
//
// 2.)After receiving a cid it creates a:
//
//	CIdInfo struct {...} instance
//
// which documents basic info about a specific CID
//
// 3.) Create a:
//
//	CidFetchResults struct {...}
//
// which contains basic info about the fetching process (pinging)
//
// 4.) Calls the:
//
//	func provide(...) inside this package cid-tracker.go
//
// providing the CID to the network
//
// 5.) adds the metrics received from func provide(...) and the fetch result struct to the newly created:
//
//	var cidInfo *CidInfo
//
// 6.) Access the tracker's field:
//
//	DBCli     *db.DBClient
//
// and adds the cidInfo and the fetchRes
//
// 7.) Adds the cid info to the tracker's:
//
//	CidPinger *CidPinger
//
// to be later pinged by the pinger.
func (publisher *CidPublisher) publishingProcess(
	publisherWG *sync.WaitGroup,
	generationDone *bool,
	publisherID int,
	cidChannel <-chan *src.TrackableCid,
	cidFetchRes *sync.Map) {
	defer func() {
		publisherWG.Done()
	}()
	logEntry := log.WithField("publisherID", publisherID)
	logEntry.Debugf("publisher ready")
	for {
		// check if the generation is done to finish the publisher (with priority)
		if *generationDone && len(cidChannel) == 0 {
			log.Info("generation process has finished and no cid is waiting to be published, closing publishingProcess")
			return
		}
		select {
		case receivedType := <-cidChannel: //this channel receives the CID from the CID generator go routine
			receivedCid := &receivedType.CID
			logEntry.Debugf("new cid to publish %s", receivedCid.Hash().B58String())
			receivedCidStr := receivedCid.Hash().B58String()

			pubTime := time.Now()

			// generate the new CidInfo cause a new CID was just received
			//TODO the content type is not necessarily random content
			cidInfo := models.NewCidInfo(*receivedCid, publisher.ReqInterval, publisher.StudyDuration, config.RandomContent, publisher.CidSource.Type(), publisher.host.ID())

			// generate the cidFetcher
			publisher.CidMap.Store(receivedCidStr, cidInfo)

			// generate a new CidFetchResults
			fetchRes := models.NewCidFetchResults(*receivedCid, pubTime, 0) // First round = Publish PR
			cidFetchRes.Store(receivedCidStr, fetchRes)

			// necessary stuff to get the different hop measurements
			lookupMetrics := dht.NewLookupMetrics()
			// currently linking a ContextKey variable througth the context that we generate
			ctx := context.WithValue(publisher.ctx, dht.ContextKey("lookupMetrics"), lookupMetrics)

			reqTime, err := provide(ctx, publisher, receivedCid)
			if err != nil {
				logEntry.Errorf("unable to Provide content. %s", err.Error())
			}
			// add the number of hops to the fetch results
			fetchRes.TotalHops = lookupMetrics.GetHops()
			fetchRes.HopsToClosest = lookupMetrics.GetHopsForPeerSet(lookupMetrics.GetClosestPeers())

			// TODO: fix this little wait to comput last PR Holder status
			// little not inside the CID to notify when k peers where recorded?
			time.Sleep(500 * time.Millisecond)

			// add the request time to the CidInfo
			cidInfo.AddPublicationTime(pubTime)
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
					receivedCid, tot, success, failed)
			}

			// send the cid_info to the cid_pinger adn start pinging PR Holders
			publisher.CidPinger.AddCidInfo(cidInfo)

		case <-publisher.ctx.Done():
			logEntry.WithField("publisherID", publisherID).Debugf("shutdown detected, closing publisher")
			return
		default:
			// keep checking if the generation has ended to close the routine
		}
	}
}

// provide calls:
//
//	DHT.Provide(...) method to provide the cid to the network
//
// and documents the time it took to publish a CID
func provide(ctx context.Context, publisher *CidPublisher, receivedCid *cid.Cid) (time.Duration, error) {
	log.Debug("calling provide method")
	tstart := time.Now()
	err := publisher.host.DHT.Provide(ctx, *receivedCid, true)
	if err != nil {
		return -1, err
	}
	reqTime := time.Since(tstart)
	return reqTime, nil
}
