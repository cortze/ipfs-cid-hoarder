package hoarder

import (
	"context"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"

	src "github.com/cortze/ipfs-cid-hoarder/pkg/cid-source"
	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
)

type CidDiscoverer struct {
	*CidTracker
	m      sync.Mutex
	CidMap map[string][]*src.TrackableCid
}

func NewCidDiscoverer(tracker *CidTracker) (*CidDiscoverer, error) {
	log.Info("Creating a new CID discoverer")

	return &CidDiscoverer{
		CidTracker: tracker,
		CidMap:     make(map[string][]*src.TrackableCid),
	}, nil
}

func (discoverer *CidDiscoverer) httpRun() {
	trackableCidsChannel := make(chan []src.TrackableCid, discoverer.Workers)
	// CID generator
	var genWG sync.WaitGroup
	genWG.Add(1)
	go discoverer.generateCidsHttp(&genWG, trackableCidsChannel)
	var addProviderWG sync.WaitGroup
	go discoverer.addProviderRecordsHttp(&addProviderWG, trackableCidsChannel)
	genWG.Wait()
	addProviderWG.Wait()
	err := discoverer.host.Close()
	if err != nil {
		log.Errorf("failed to close host: %s", err)
		return
	}
	log.Debug("Closed the discoverer host")
}

func (discoverer *CidDiscoverer) run() {
	defer discoverer.wg.Done()
	// launch the PRholder reading routine
	//msgNotChannel := discoverer.MsgNot.GetNotifierChan()

	if discoverer.CidSource.Type() == config.HttpServerSource {
		discoverer.httpRun()
		return
	}
	trackableCidC := make(chan *src.TrackableCid, discoverer.Workers)
	// CID generator
	var genWG sync.WaitGroup
	genWG.Add(1)
	go discoverer.generateCids(&genWG, trackableCidC)

	var addProviderWG sync.WaitGroup
	go discoverer.addProvider(&addProviderWG, trackableCidC)
	genWG.Wait()
	addProviderWG.Wait()

	log.Debugf("Number of cids read was: %d", len(discoverer.CidMap))
	var discovererWG sync.WaitGroup

	for cidstr, getNewCidReturnTypeArray := range discoverer.CidMap {
		discovererWG.Add(1)
		go discoverer.discoveryProcess(&discovererWG, cidstr, getNewCidReturnTypeArray)
	}

	discovererWG.Wait()
	//close(msgNotChannel)
	//log.Debug("CLOSED NOT CHANNEL")
	err := discoverer.host.Close()
	if err != nil {
		log.Errorf("failed to close host: %s", err)
		return
	}
	log.Debug("Closed the discoverer host")
}

func (discoverer *CidDiscoverer) addToMap(trackableCid *src.TrackableCid) {
	cidStr := trackableCid.CID.Hash().B58String()
	if typeInstance, ok := discoverer.CidMap[cidStr]; ok {
		discoverer.CidMap[cidStr] = append(typeInstance, trackableCid)
	} else {
		arr := make([]*src.TrackableCid, 0)
		arr = append(arr, trackableCid)
		discoverer.CidMap[cidStr] = arr
	}
}

func (discoverer *CidDiscoverer) addProvider(addProviderWG *sync.WaitGroup, trackableCidC <-chan *src.TrackableCid) {
	defer addProviderWG.Done()
	ctx := discoverer.ctx
	for {
		select {
		case trackableCid, ok := <-trackableCidC:
			if !ok {
				break
			}
			if trackableCid.IsEmpty() {
				break
			}

			cidStr := trackableCid.CID.Hash().B58String()

			log.Debugf(
				"New provide and CID received from channel. Cid:%s,Pid:%s,Mutliaddresses:%v,ProvideTime:%s,UserAgent:%s",
				cidStr, trackableCid.ID.String(),
				trackableCid.Addresses, trackableCid.ProvideTime, trackableCid.UserAgent,
			)
			discoverer.m.Lock()
			discoverer.addToMap(trackableCid)

			err := addPeerToProviderStore(ctx, discoverer.host, trackableCid.ID, trackableCid.CID, trackableCid.Addresses)
			if err != nil {
				log.Errorf("error %s calling addpeertoproviderstore method", err)
			} else {
				log.Debug("Added providers to provider store")
			}

			err = addAgentVersionToProvideStore(discoverer.host, trackableCid.ID, trackableCid.UserAgent)

			if err != nil {
				log.Errorf("error %s calling addAgentVersionToProvideStore", err)
			} else {
				log.Debug("Added agent version to provider store")
			}

			discoverer.m.Unlock()

		case <-ctx.Done():
			log.Debugf("shutdown detected, closing discoverer through add provider")
			return
		default:
			//log.Debug("haven't received anything yet")
		}
	}
}

func (discoverer *CidDiscoverer) addProviderRecordsHttp(addProviderWG *sync.WaitGroup, trackableCidsChannel <-chan []src.TrackableCid) {
	defer addProviderWG.Done()
	ctx := discoverer.ctx
	for {
		select {
		case trackableCids, ok := <-trackableCidsChannel:
			if !ok {
				break
			}
			if trackableCids == nil {
				break
			}

			tr := trackableCids[0]
			cidStr := tr.CID.Hash().B58String()

			log.Debugf(
				"New provide and CID received from channel. Cid:%s,Pid:%s,Mutliaddresses:%v,ProvideTime:%s,UserAgent:%s",
				cidStr, tr.ID.String(),
				tr.Addresses, tr.ProvideTime, tr.UserAgent,
			)

			//the starting values for the discoverer
			cidIn, err := cid.Parse(cidStr)

			if err != nil {
				log.Errorf("couldnt parse cid")
			}

			cidInfo := models.NewCidInfo(cidIn, discoverer.ReqInterval, discoverer.StudyDuration, config.JsonFileSource,
				discoverer.CidSource.Type(), "")
			fetchRes := models.NewCidFetchResults(cidIn, 0)

			// generate a new CidFetchResults
			//TODO starting data for the discoverer
			fetchRes.TotalHops = 0
			fetchRes.HopsToClosest = 0
			for _, trackableCid := range trackableCids {

				err := addPeerToProviderStore(ctx, discoverer.host, trackableCid.ID, trackableCid.CID, trackableCid.Addresses)
				if err != nil {
					log.Errorf("error %s calling addpeertoproviderstore method", err)
				} else {
					log.Debug("Added providers to provider store")
				}

				err = addAgentVersionToProvideStore(discoverer.host, trackableCid.ID, trackableCid.UserAgent)

				if err != nil {
					log.Errorf("error %s calling addAgentVersionToProvideStore", err)
				} else {
					log.Debug("Added agent version to provider store")
				}
				cidInfo.AddPublicationTime(trackableCid.PublicationTime)
				cidInfo.AddProvideTime(trackableCid.ProvideTime)

				//TODO discoverer starting ping res
				pingRes := models.NewPRPingResults(
					cidIn,
					trackableCid.ID,
					//the below are starting data for the discoverer
					0,
					time.Time{},
					0,
					true,
					true,
					p2p.NoConnError,
				)
				cidInfo.AddCreator(trackableCid.Creator)
				fetchRes.AddPRPingResults(pingRes)

				log.Debugf("User agent received from provider store: %s", discoverer.host.GetUserAgentOfPeer(trackableCid.ID))

				prHolderInfo := models.NewPeerInfo(
					trackableCid.ID,
					discoverer.host.Peerstore().Addrs(trackableCid.ID),
					discoverer.host.GetUserAgentOfPeer(trackableCid.ID),
				)

				cidInfo.AddPRHolder(prHolderInfo)
			}
			cidInfo.AddPRFetchResults(fetchRes)

			discoverer.DBCli.AddCidInfo(cidInfo)
			discoverer.DBCli.AddFetchResult(fetchRes)

			discoverer.CidPinger.AddCidInfo(cidInfo)

		case <-ctx.Done():
			log.Debugf("shutdown detected, closing discoverer through add provider")
			return
		default:
			//log.Debug("haven't received anything yet")
		}
	}
}

// This method essentially initializes the data for the pinger to be able to get information about the PR holders later.
func (discoverer *CidDiscoverer) discoveryProcess(discovererWG *sync.WaitGroup, cidstr string, trackableCidArr []*src.TrackableCid) {
	defer discovererWG.Done()
	//the starting values for the discoverer
	cidIn, err := cid.Parse(cidstr)

	if err != nil {
		log.Errorf("couldnt parse cid")
	}

	cidInfo := models.NewCidInfo(cidIn, discoverer.ReqInterval, discoverer.StudyDuration, config.JsonFileSource, discoverer.CidSource.Type(), "")
	fetchRes := models.NewCidFetchResults(cidIn, 0)

	// generate a new CidFetchResults
	//TODO starting data for the discoverer
	fetchRes.TotalHops = 0
	fetchRes.HopsToClosest = 0
	for _, val := range trackableCidArr {
		cidInfo.AddPublicationTime(val.PublicationTime)
		cidInfo.AddProvideTime(val.ProvideTime)
		//TODO discoverer starting ping res
		pingRes := models.NewPRPingResults(
			cidIn,
			val.ID,
			//the below are starting data for the discoverer
			0,
			time.Time{},
			0,
			true,
			true,
			p2p.NoConnError,
		)
		cidInfo.AddCreator(val.Creator)
		fetchRes.AddPRPingResults(pingRes)

		log.Debugf("User agent received from provider store: %s", discoverer.host.GetUserAgentOfPeer(val.ID))

		prHolderInfo := models.NewPeerInfo(
			val.ID,
			discoverer.host.Peerstore().Addrs(val.ID),
			discoverer.host.GetUserAgentOfPeer(val.ID),
		)

		cidInfo.AddPRHolder(prHolderInfo)
	}

	cidInfo.AddPRFetchResults(fetchRes)

	tot, success, failed := cidInfo.GetFetchResultSummaryOfRound(0)
	if tot < 0 {
		log.Warnf("no ping results for the PR provide round of Cid %s", cidInfo.CID.Hash().B58String())
	} else {
		log.Infof("Cid %s - %d total PRHolders | %d successfull PRHolders | %d failed PRHolders",
			cidIn, tot, success, failed)
	}

	discoverer.DBCli.AddCidInfo(cidInfo)
	discoverer.DBCli.AddFetchResult(fetchRes)

	discoverer.CidPinger.AddCidInfo(cidInfo)
}

/*
func addAddrtoPeerstore(h host.Host, pid peer.ID, multiaddr []ma.Multiaddr) {
	h.Peerstore().AddAddrs(pid, multiaddr, peerstore.PermanentAddrTTL)
}
*/

// Instead of adding directly to peerstore the API is the following
func addPeerToProviderStore(ctx context.Context, h *p2p.Host, pid peer.ID, cid cid.Cid, multiaddr []ma.Multiaddr) error {
	keyMH := cid.Hash()
	err := h.DHT.ProviderStore().AddProvider(ctx, keyMH, peer.AddrInfo{ID: pid, Addrs: multiaddr})
	if err != nil {
		return errors.Wrap(err, " while trying to add provider to peerstore")
	}
	return nil
}

func addAgentVersionToProvideStore(h *p2p.Host, pid peer.ID, useragent string) error {
	return h.Peerstore().Put(pid, "AgentVersion", useragent)
}
