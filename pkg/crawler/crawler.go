package crawler

import (
	"context"
	"fmt"
	"strings"
	"time"

	"ipfs-cid-hoarder/pkg/config"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	kdht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"

	ma "github.com/multiformats/go-multiaddr"

	libp2p "github.com/libp2p/go-libp2p"
)

// Crawler is simply a wrappper on top of the official go-lip2p-kad-dht/crawler
// It is mainly used to crawl the network searching for Hydra-Booster peers.
// with the final intention of blacklisting them in the Kad-Dht process if the Avoid-Hydras flags gets triggered
type Crawler struct {
	ctx context.Context

	crawler     *crawler.Crawler
	h           host.Host
	results     *CrawlResults
	blacklistUA string
}

func New(ctx context.Context, privKey crypto.PrivKey, ip, port, blacklistUA string) (*Crawler, error) {
	log.Debug("generating new crawler for the Cid-Hoarder")

	// compose the multiaddress
	mAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", ip, port))
	if err != nil {
		return nil, err
	}

	// generate the libp2p host
	h, err := libp2p.New(
		libp2p.ListenAddrs(mAddr),
		libp2p.Identity(privKey),
		libp2p.UserAgent(config.UserAgent),
		libp2p.Transport(tcp.NewTCPTransport))
	if err != nil {
		return nil, err
	}

	// create the official crawler
	c, err := crawler.New(
		h,
		crawler.WithParallelism(1000),
		crawler.WithMsgTimeout(40*time.Second),
		crawler.WithConnectTimeout(40*time.Second),
	)
	if err != nil {
		return nil, err
	}

	return &Crawler{
		ctx:         ctx,
		crawler:     c,
		h:           h,
		results:     NewCrawlerResults(),
		blacklistUA: blacklistUA,
	}, nil
}

func RunLightCrawler(ctx context.Context, balcklistingUA string) (CrawlResults, error) {
	// generate private and public keys for the light crawler
	priv, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		return CrawlResults{}, errors.Wrap(err, "unable to generate priv key for lighth client")
	}

	// Initialize the CidHoarder
	c, err := New(ctx, priv, config.CliIp, config.CliPort, balcklistingUA)
	if err != nil {
		return CrawlResults{}, err
	}

	log.Info("running cid-hoarder crawler")

	results := c.run()

	log.WithFields(log.Fields{
		"total":       len(results.GetDiscvPeers()),
		"active":      len(results.GetActivePeers()),
		"blacklisted": len(results.GetBlacklistedPeers()),
		"failed":      len(results.GetFailedPeers()),
		"time":        results.GetCrawlerDuration(),
	}).Info("crawl summary")

	return *results, nil
}

func (c *Crawler) run() *CrawlResults {
	logEntry := log.WithFields(log.Fields{
		"module": "crawler",
	})

	// set up the handle Success function for the crawler
	handleSucc := func(p peer.ID, rtPeers []*peer.AddrInfo) {
		logEntry.Debugf("peer %s successfully connected", p.String())

		// add the peer `P` to the list of active peers
		c.results.addPeer(p, active)

		// add each of the `rtPeers` to the discovered peers
		for _, pi := range rtPeers {
			c.results.addPeer(pi.ID, disc)
		}

		// check if the host knows the UserAgent of `P` to see if it matches the blacklisted peers
		// and add it to the blacklisted peer list
		ua, err := c.h.Peerstore().Get(p, "AgentVersion")
		if err == nil && c.blacklistUA != "" { // if there was no error and blacklistUA was defined by user
			userAgent := ua.(string)
			if strings.Contains(userAgent, c.blacklistUA) {
				logEntry.Debugf("peer %s was blacklisted -> useragent %s", p.String(), userAgent)
				c.results.addPeer(p, blacklist)
			}
		}
	}

	// set up the handle Fail function for the crawler
	handleFail := func(p peer.ID, err error) {
		logEntry.Debugf("peer %s wasn't diable", p.String())
		c.results.addPeer(p, failed)
	}

	// run the go-libp2p-kad-dht crawler
	logEntry.Debug("running crawler for the cid-hoarder")

	c.results.initTime = time.Now()
	c.crawler.Run(c.ctx, kdht.GetDefaultBootstrapPeerAddrInfos(), handleSucc, handleFail)
	c.results.finishTime = time.Now()

	return c.results
}

func (c *Crawler) Close() {
	// check is there is host
	if c.h == nil {
		log.Warn("attempting to close non-existing host")
		return
	}
	c.h.Close()
}
