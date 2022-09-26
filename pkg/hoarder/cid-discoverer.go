package hoarder

import (
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	ma "github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
)

func NewCidDiscover(tracker *CidTracker) (*CidDiscoverer, error) {
	log.Debug("Creating a new CID discoverer")
	return &CidDiscoverer{
		CidTracker: tracker,
	}, nil
}

func (discoverer *CidDiscoverer) run() {

	provideAndCidsChannel := make(chan *ProviderAndCID, discoverer.Workers)
	// CID generator
	var genWG sync.WaitGroup
	genWG.Add(1)
	go discoverer.readCIDs(discoverer.CidSource, &genWG, provideAndCidsChannel)

	var discovererWG sync.WaitGroup

	// CID PR Discoverers which are essentially the workers of tracker.
	for discovererCounter := 0; discovererCounter < discoverer.Workers; discovererCounter++ {
		discovererWG.Add(1)
		// start the discovery process
		go discoverer.discovery_process(&discovererWG, discovererCounter, provideAndCidsChannel)
	}
}

//This method essentially initiliazes the data for the pinger to be able to get information about the PR holders later.
func (discoverer *CidDiscoverer) discovery_process(discovererWG *sync.WaitGroup, discovererID int, providerAndCidChannel chan *ProviderAndCID) {
	defer discovererWG.Done()
	logEntry := log.WithField("discoverer ID", discovererID)
	ctx := discoverer.ctx
	logEntry.Debugf("discoverer ready")
	for {
		select {
		case received_ProvideAndCid := <-providerAndCidChannel:
			cidStr := received_ProvideAndCid.CID.Hash().B58String()
			logEntry.Debugf(
				"New provide and CID received from channel. Cid:%s,Pid:%s,Mutliaddresses:%v",
				cidStr, received_ProvideAndCid.ID.String(),
				received_ProvideAndCid.Addresses,
			)
			//TODO what will the discoverer have instead of the request interval
			//the starting values for the discoverer
			cidInfo := models.NewCidInfo(received_ProvideAndCid.CID, discoverer.ReqInterval, config.TextFileSource, discoverer.CidSource.Type(), discoverer.host.ID())
			fetchRes := models.NewCidFetchResults(received_ProvideAndCid.CID, 0) // First round = Publish PR

			discoverer.ProviderAndCID.Store(cidStr, fetchRes)
			cidInfo.AddProvideTime(0)
			// generate a new CidFetchResults
			//TODO starting data for the discoverer
			fetchRes.TotalHops = 0
			fetchRes.HopsToClosest = 0
			cidInfo.AddPRFetchResults(fetchRes)
			discoverer.DBCli.AddCidInfo(cidInfo)
			discoverer.DBCli.AddFetchResult(fetchRes)
			//TODO discoverer starting ping res
			pingRes := models.NewPRPingResults(
				received_ProvideAndCid.CID,
				received_ProvideAndCid.ID,
				//the below are starting data for the discoverer
				0,
				time.Time{},
				0,
				true,
				true,
				p2p.NoConnError,
			)
			fetchRes.AddPRPingResults(pingRes)
			//TODO will this work, because no add provider RPC has been completed
			addAddrtoPeerstore(discoverer.host, received_ProvideAndCid.ID, received_ProvideAndCid.Addresses)
			useragent := discoverer.host.GetUserAgentOfPeer(received_ProvideAndCid.ID)

			prHolderInfo := models.NewPeerInfo(
				received_ProvideAndCid.ID,
				//TODO is it in the peerstore of the host
				discoverer.host.Peerstore().Addrs(received_ProvideAndCid.ID),
				useragent,
			)
			cidInfo.AddPRHolder(prHolderInfo)
			discoverer.CidPinger.AddCidInfo(cidInfo)
		case <-ctx.Done():
			logEntry.WithField("discovererID", discovererID).Debugf("shutdown detected, closing discoverer")
			return
		}
	}
}

func addAddrtoPeerstore(h host.Host, pid peer.ID, multiaddr []ma.Multiaddr) {
	h.Peerstore().AddAddrs(pid, multiaddr, peerstore.PermanentAddrTTL)
}
