package p2p

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"ipfs-cid-hoarder/pkg/config"
	"ipfs-cid-hoarder/pkg/crawler"

	log "github.com/sirupsen/logrus"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"

	//quic "github.com/libp2p/go-libp2p-quic-transport"
	tcp "github.com/libp2p/go-tcp-transport"
	ma "github.com/multiformats/go-multiaddr"
)

type Host struct {
	ctx context.Context

	host.Host
	DHT         *kaddht.IpfsDHT
	MsgNotifier *Notifier
	// TODO: DB client
	StartTime time.Time
}

func NewHost(ctx context.Context, privKey crypto.PrivKey, ip, port string, bucketSize int, hydraFilter bool) (*Host, error) {
	log.Debug("Creating host")

	// set the max limit of connections to 30000
	os.Setenv("LIBP2P_SWARM_FD_LIMIT", "30000")

	// compose the multiaddress
	mAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", ip, port))
	if err != nil {
		return nil, err
	}

	var dht *kaddht.IpfsDHT
	// check if hydra filter has been tuned
	var blacklistedUA string
	var blacklistPeers map[peer.ID]struct{}

	if hydraFilter {
		log.Info("hydra-filter: ON -> crawling network to identify hydra-boosters (might take 5-7mins)")
		blacklistedUA = config.DefaultBlacklistUserAgent
		// launch light crawler identifying balcklistable peers
		crawlResutls, err := crawler.RunLightCrawler(ctx, blacklistedUA)
		if err != nil {
			return nil, err
		}
		blacklistPeers = crawlResutls.GetBlacklistedPeers()
	}
	msgSender := NewCustomMessageSender(blacklistedUA)

	// generate the libp2p host
	h, err := libp2p.New(
		libp2p.ListenAddrs(mAddr),
		libp2p.Identity(privKey),
		libp2p.UserAgent(config.UserAgent),
		libp2p.Transport(tcp.NewTCPTransport),
		//libp2p.Transport(quic.NewTransport), // not supported in 1.18 yet
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			dht, err = kaddht.New(ctx, h,
				kaddht.Mode(kaddht.ModeClient),
				// Consider a Wrapper around MessageSender to get more details of underneath processes
				kaddht.WithCustomMessageSender(msgSender.Init),
				kaddht.BucketSize(bucketSize),
				kaddht.WithPeerBlacklist(blacklistPeers), // always blacklist (can be an empty map or a full one)
			)
			return dht, err
		}),
	)
	if err != nil {
		return nil, err
	}

	// check if the DHT is empty or not
	if dht == nil {
		return nil, errors.New("error - no IpfsDHT server has been initialized")
	}
	hw := &Host{
		ctx:         ctx,
		Host:        h,
		MsgNotifier: msgSender.GetMsgNotifier(),
		DHT:         dht,
		StartTime:   time.Now(),
	}

	log.Debugf("new host generated - peerID %s", h.ID().String())
	return hw, nil
}

func (h *Host) PeerId() peer.ID {
	return h.Host.ID()
}

func (h *Host) GetMsgNotifier() *Notifier {
	return h.MsgNotifier
}

func (h *Host) Boostrap(ctx context.Context) error {
	succCon := int64(0)

	log.Info("Initializing Boostrap with KadDHT default nodes")
	var wg sync.WaitGroup

	for _, bnode := range kaddht.GetDefaultBootstrapPeerAddrInfos() {
		wg.Add(1)
		go func(bn peer.AddrInfo) {
			defer wg.Done()
			log.Infof("connecting bootstrap node: %s", bn.String())
			err := h.Host.Connect(ctx, bn)
			if err != nil {
				log.Errorf("unable to connect bnode %s", bn.String())
			} else {
				atomic.AddInt64(&succCon, 1)
			}
		}(bnode)
	}

	wg.Wait()

	if succCon > 0 {
		log.Infof("got connected to %d bootstrap nodes", succCon)
	} else {
		return errors.New("unable to connect any of the bootstrap nodes from KDHT")
	}
	return nil
}

func (h *Host) GetUserAgentOfPeer(p peer.ID) (useragent string) {
	userAgentInterf, err := h.Host.Peerstore().Get(p, "AgentVersion")
	if err != nil {
		useragent = NoUserAgentDefined
	} else {
		useragent = userAgentInterf.(string)
	}
	return
}
