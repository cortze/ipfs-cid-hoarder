package hoarder

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	log "github.com/sirupsen/logrus"
)

var DialTimeout = 20 * time.Second

type CidFetcher struct {
	ctx context.Context
	wg  *sync.WaitGroup

	host         *p2p.Host
	dbCli        *db.DBClient
	pingInterval time.Duration

	// control variables
	cidInfo        *models.CidInfo
	maxPingCounter int // number of pings for the same round (starting from 0)
}

//
func NewCidFetcher(
	ctx context.Context,
	wg *sync.WaitGroup,
	h *p2p.Host,
	db *db.DBClient,
	cidInfo *models.CidInfo,
	reqFreq time.Duration,
	maxPingCnt int) *CidFetcher {

	return &CidFetcher{
		ctx:            ctx,
		wg:             wg,
		host:           h,
		dbCli:          db,
		pingInterval:   reqFreq,
		maxPingCounter: maxPingCnt,
		cidInfo:        cidInfo,
	}
}

func (f *CidFetcher) Run() {
	defer f.wg.Done()
	logEntry := log.WithFields(log.Fields{
		"cid": f.cidInfo.CID.Hash().B58String(),
		"k":   f.cidInfo.K,
	})

	logEntry.Infof("launching fetcher for Cid %s", f.cidInfo.CID.Hash().B58String())

	// generate the tickers to ping the cidHolders
	intvalTicker := time.NewTicker(f.pingInterval)

	pingCounter := 1
	for {
		select {
		case <-intvalTicker.C: // ping the cid holder
			cidFetchRes := models.NewCidFetchResults(f.cidInfo.CID, pingCounter)

			var wg sync.WaitGroup

			for _, peerInfo := range f.cidInfo.PRHolders {
				// launch in parallel all the peer Pings
				wg.Add(1)
				go func(wg *sync.WaitGroup, c cid.Cid, peerInfo *models.PeerInfo, fetchRes *models.CidFetchResults) {
					defer wg.Done()
					pingRes := f.PingPRHolder(c, pingCounter, peerInfo.GetAddrInfo())
					fetchRes.AddPRPingResults(pingRes)
				}(&wg, f.cidInfo.CID, peerInfo, cidFetchRes)
			}

			wg.Wait()

			// add the fetch results to the array and persist it in the DB
			f.cidInfo.AddPRFetchResults(cidFetchRes)
			err := f.dbCli.AddFetchResults(cidFetchRes)
			if err != nil {
				log.Fatalln("unable to persist to DB new fetch_results", err)
			}
			err = f.dbCli.AddPingResultsSet(cidFetchRes.PRPingResults)
			if err != nil {
				log.Fatalln("unable to persist to DB new ping_results", err)
			}

			// TODO: making a DHT lookup for the content takes too much?¿?
			// wg.Add(1)
			// go func(ctx context.Context, wg sync.WaitGroup, h *p2p.Host, c cid.Cid, cidFetchRes *models.CidFetchResults) {
			// 	defer wg.Done()
			// 	// check if the content is retrievable in the network
			// 	providers, err := h.DHT.FindProviders(ctx, c)
			// 	if err != nil {
			// 		log.Errorf("unable to find PR for Cid %s", c.Hash().B58String())
			// 	}
			// 	// TODO: possible race condition in the future
			// 	cidFetchRes.IsRetrievable = false
			// 	if len(providers) > 0 {
			// 		cidFetchRes.IsRetrievable = true
			// 	}
			// }(f.ctx, wg, f.host, f.cidInfo.CID, cidFetchRes)

			// wg.Wait()

			// TODO:	-Add the new closest peers to the content? (to track the degradation of the Provider Record)
			logEntry.Infof("ping round %d", pingCounter)

			pingCounter++
			if pingCounter > f.maxPingCounter {

				logEntry.Infof("reached limit of pings (%d)", pingCounter)
				summary := "fetch results of CID" + " \n"

				// log out the status of the fetchings
				for idx, fetchRes := range f.cidInfo.PRPingResults {
					tot, suc, fail := fetchRes.GetSummary()
					if tot < 0 {
						logEntry.Errorf("unable to retrieve the fetch result %d - actual len of fetching results %d\n", idx, f.cidInfo.GetFetchResultsLen())
					}
					summary += fmt.Sprintf("ping %d | total %d | success %d | failed %d\n", idx, tot, suc, fail)
				}
				logEntry.Infof("summary:\n%s", summary)
				return
			}

		case <-f.ctx.Done():
			logEntry.Debug("closing fetcher")
			return
		}
	}

}

// main function that will
func (f *CidFetcher) PingPRHolder(c cid.Cid, round int, pAddr peer.AddrInfo) *models.PRPingResults {
	logEntry := log.WithFields(log.Fields{
		"cid": f.cidInfo.CID.Hash().B58String(),
		"k":   f.cidInfo.K,
	})

	var active, hasRecords bool
	var connError string

	tstart := time.Now()
	// connect the peer
	provs, _, err := f.host.DHT.GetProvidersFromPeer(f.ctx, pAddr.ID, c.Hash())
	fetchTime := time.Since(tstart)
	if err != nil {
		logEntry.Debugf("unable to connect peer %s for Cid %s - error %s", pAddr.ID.String(), c.Hash().B58String(), err.Error())
		connError = err.Error() // TODO: IPFSdht error parsing missing
	} else {
		logEntry.Debugf("succesful connection to peer %s for Cid %s", pAddr.ID.String(), c.Hash().B58String())
		active = true
		connError = p2p.NoConnError
		// check it the peer still has the records
		if len(provs) > 0 {
			hasRecords = true
		}
		logEntry.Debugf("providers for Cid %s from peer %s - %v\n", c.Hash().B58String(), pAddr.ID.String(), provs)
	}

	return models.NewPRPingResults(
		c,
		pAddr.ID,
		round,
		fetchTime,
		active,
		hasRecords,
		connError)
}
