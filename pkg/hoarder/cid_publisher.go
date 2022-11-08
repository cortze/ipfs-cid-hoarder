package hoarder

import (
	"context"
	src "ipfs-cid-hoarder/pkg/cid-source"
	"reflect"
	"sync"
	"time"

	"ipfs-cid-hoarder/pkg/config"
	"ipfs-cid-hoarder/pkg/models"
	"ipfs-cid-hoarder/pkg/p2p"

	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	log "github.com/sirupsen/logrus"
)

func NewCidPublisher(tracker *CidTracker, h *p2p.Host, cidNum int) (*CidPublisher, error) {
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

	var firstCidFetchRes sync.Map

	// generate a channel with the same size as the Workers one
	getNewCidReturnTypeChannel := make(chan *src.GetNewCidReturnType, publisher.Workers)

	// IPFS DHT Message Notification Listener
	done := make(chan struct{})
	go publisher.addProviderMsgListener(&firstCidFetchRes, done, msgNotChannel)
	// CID generator
	var genWG sync.WaitGroup
	genWG.Add(1)
	go publisher.generateCids(&genWG, getNewCidReturnTypeChannel)

	var publisherWG sync.WaitGroup

	// CID PR Publishers which are essentially the workers of tracker.
	for publisherCounter := 0; publisherCounter < publisher.Workers; publisherCounter++ {
		publisherWG.Add(1)
		// start the providing process
		go publisher.publishingProcess(&publisherWG, publisherCounter, getNewCidReturnTypeChannel, &firstCidFetchRes)
	}
	genWG.Wait()
	publisherWG.Wait()
	done <- struct{}{}
	//close the publisher host
	err := publisher.host.Close()
	if err != nil {
		log.Errorf("failed to close host: %s", err)
		return
	}
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
func (publisher *CidPublisher) addProviderMsgListener(firstCidFetchRes *sync.Map, done chan struct{}, msgNotChannel <-chan *p2p.MsgNotification) {
	for {
		select {
		case msgNot, ok := <-msgNotChannel: //this receives a message from SendMessage in messages.go after the DHT.Provide operation is called from the PUT_PROVIDER method.

			if !ok {
				log.Warn("msg Not channel is closed, closing reader for PR holders")
				return
			}
			if msgNot == nil {
				log.Warn("empty msgNot Received, closing reader for PR Holders")
				return
			}
			casted_cid, err := cid.Cast(msgNot.Msg.GetKey())
			if err != nil {
				log.Errorf("unable to cast msg key into cid. %s", err.Error())
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

				//if no error occurred in p2p.MsgNotification the ping result will contain active = true and hasRecords = true,
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
				//log.Debugf("msg is not ADD_PROVIDER msg. It is of type %s", msgNot.Msg.Type.String())
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
func (publisher *CidPublisher) publishingProcess(publisherWG *sync.WaitGroup, publisherID int, cidChannel <-chan *src.GetNewCidReturnType, cidFetchRes *sync.Map) {
	defer publisherWG.Done()
	logEntry := log.WithField("publisherID", publisherID)
	ctx := publisher.ctx
	logEntry.Debugf("publisher ready")
	for {

		select {
		case receivedType, ok := <-cidChannel: //this channel receives the CID from the CID generator go routine

			if !ok {
				log.Warn("Cid channel has been closed")
				return
			}

			if reflect.DeepEqual(*receivedType, src.Undef) {
				log.Warn("Received undef type from cid channel")
				return
			}

			receivedCid := &receivedType.CID
			logEntry.Debugf("new cid to publish %s", receivedCid.Hash().B58String())
			receivedCidStr := receivedCid.Hash().B58String()
			// generate the new CidInfo cause a new CID was just received
			//TODO the content type is not necessarily random content
			cidInfo := models.NewCidInfo(*receivedCid, publisher.ReqInterval, config.RandomContent, publisher.CidSource.Type(), publisher.host.ID())

			// generate the cidFetcher
			publisher.CidMap.Store(receivedCidStr, cidInfo)

			// generate a new CidFetchResults
			fetchRes := models.NewCidFetchResults(*receivedCid, 0) // First round = Publish PR
			cidFetchRes.Store(receivedCidStr, fetchRes)

			// necessary stuff to get the different hop measurements
			var hops dht.Hops
			// currently linking a ContextKey variable througth the context that we generate
			ctx := context.WithValue(publisher.ctx, dht.ContextKey("hops"), &hops)

			reqTime, err := provide(ctx, publisher, receivedCid)
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
					receivedCid, tot, success, failed)
			}

			// send the cid_info to the cid_pinger adn start pinging PR Holders
			publisher.CidPinger.AddCidInfo(cidInfo)

		case <-ctx.Done():
			logEntry.WithField("publisherID", publisherID).Debugf("shutdown detected, closing publisher")
			return
		}
	}
}

//provide calls:
//	DHT.Provide(...) method to provide the cid to the network
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
