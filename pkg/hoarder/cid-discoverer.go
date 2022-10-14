package hoarder

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"

	src "ipfs-cid-hoarder/pkg/cid-source"
	"ipfs-cid-hoarder/pkg/config"
	"ipfs-cid-hoarder/pkg/models"
	"ipfs-cid-hoarder/pkg/p2p"

	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	log "github.com/sirupsen/logrus"
)

func NewCidDiscoverer(tracker *CidTracker) (*CidDiscoverer, error) {
	log.Info("Creating a new CID discoverer")
	return &CidDiscoverer{
		CidTracker: tracker,
		CidMap:     make(map[string][]*src.GetNewCidReturnType),
	}, nil
}

func (discoverer *CidDiscoverer) run() {

	getNewCidReturnTypeChannel := make(chan *src.GetNewCidReturnType, discoverer.Workers)
	// CID generator
	var genWG sync.WaitGroup
	genWG.Add(1)
	go discoverer.generateCids(&genWG, getNewCidReturnTypeChannel)

	var addProviderWG sync.WaitGroup
	go discoverer.addProvider(&addProviderWG, getNewCidReturnTypeChannel)
	genWG.Wait()
	addProviderWG.Wait()

	var discovererWG sync.WaitGroup

	for cidstr, getNewCidReturnTypeArray := range discoverer.CidMap {
		discovererWG.Add(1)
		discoverer.discoveryProcess(&discovererWG, cidstr, getNewCidReturnTypeArray)
	}

	discovererWG.Wait()
	err := discoverer.host.Close()
	if err != nil {
		return
	}
}

func (discoverer *CidDiscoverer) addToMap(getNewCidReturnTypeInstance *src.GetNewCidReturnType) {
	cidStr := getNewCidReturnTypeInstance.CID.Hash().B58String()
	if typeInstance, ok := discoverer.CidMap[cidStr]; ok {
		typeInstance = append(typeInstance, getNewCidReturnTypeInstance)
	} else {
		arr := make([]*src.GetNewCidReturnType, 0)
		arr = append(arr, getNewCidReturnTypeInstance)
		discoverer.CidMap[cidStr] = arr
	}
}

func (discoverer *CidDiscoverer) addProvider(addProviderWG *sync.WaitGroup, getNewCidReturnTypeChannel <-chan *src.GetNewCidReturnType) {
	defer addProviderWG.Done()
	ctx := discoverer.ctx
	for {
		select {
		case getNewCidReturnTypeInstance := <-getNewCidReturnTypeChannel:
			if reflect.DeepEqual(*getNewCidReturnTypeInstance, src.Undef) {
				break
			}
			cidStr := getNewCidReturnTypeInstance.CID.Hash().B58String()

			log.Debugf(
				"New provide and CID received from channel. Cid:%s,Pid:%s,Mutliaddresses:%v",
				cidStr, getNewCidReturnTypeInstance.ID.String(),
				getNewCidReturnTypeInstance.Addresses,
			)
			discoverer.m.Lock()
			discoverer.addToMap(getNewCidReturnTypeInstance)
			discoverer.m.Unlock()

			err := addPeerToProviderStore(ctx, discoverer.host, getNewCidReturnTypeInstance.ID, getNewCidReturnTypeInstance.CID, getNewCidReturnTypeInstance.Addresses)
			if err != nil {
				log.Errorf("error %s calling addpeertoproviderstore method", err)
			}

		case <-ctx.Done():
			log.Debugf("shutdown detected, closing discoverer through add provider")
			return
		default:
			//log.Debug("haven't received anything yet")
		}
	}
}

//This method essentially initializes the data for the pinger to be able to get information about the PR holders later.
func (discoverer *CidDiscoverer) discoveryProcess(discovererWG *sync.WaitGroup, cidstr string, getNewCidReturnTypearr []*src.GetNewCidReturnType) {
	defer discovererWG.Done()
	//the starting values for the discoverer
	cidIn, err := cid.Parse(cidstr)

	if err != nil {
		log.Errorf("couldnt parse cid")
	}

	cidInfo := models.NewCidInfo(cidIn, discoverer.ReqInterval, config.JsonFileSource, discoverer.CidSource.Type(), discoverer.host.ID())
	fetchRes := models.NewCidFetchResults(cidIn, 0)

	cidInfo.AddProvideTime(0)
	// generate a new CidFetchResults
	//TODO starting data for the discoverer
	fetchRes.TotalHops = 0
	fetchRes.HopsToClosest = 0
	for _, val := range getNewCidReturnTypearr {
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
		fetchRes.AddPRPingResults(pingRes)
		useragent := discoverer.host.GetUserAgentOfPeer(val.ID)

		prHolderInfo := models.NewPeerInfo(
			val.ID,
			discoverer.host.Peerstore().Addrs(val.ID),
			useragent,
		)

		cidInfo.AddPRHolder(prHolderInfo)
	}

	cidInfo.AddPRFetchResults(fetchRes)

	discoverer.DBCli.AddCidInfo(cidInfo)
	discoverer.DBCli.AddFetchResult(fetchRes)

	discoverer.CidPinger.AddCidInfo(cidInfo)
}

/*
func addAddrtoPeerstore(h host.Host, pid peer.ID, multiaddr []ma.Multiaddr) {
	h.Peerstore().AddAddrs(pid, multiaddr, peerstore.PermanentAddrTTL)
}
*/

//Instead of adding directly to peerstore the API is the following
func addPeerToProviderStore(ctx context.Context, h *p2p.Host, pid peer.ID, cid cid.Cid, multiaddr []ma.Multiaddr) error {
	keyMH := cid.Hash()
	err := h.DHT.ProviderStore().AddProvider(ctx, keyMH, peer.AddrInfo{ID: pid, Addrs: multiaddr})
	if err != nil {
		return errors.Wrap(err, " while trying to add provider to peerstore")
	}
	return nil
}
