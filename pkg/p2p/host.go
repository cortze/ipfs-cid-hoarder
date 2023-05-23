package p2p

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"

	libp2p "github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"

	ma "github.com/multiformats/go-multiaddr"
)

const (
	DialTimeout = 60 * time.Second
)

type HostOptions struct {
	IP string
	Port string
	K int
	BlacklistingUA string
	BlacklistedPeers map[peer.ID]struct{}
}

type Host struct {
	ctx context.Context

	host.Host
	DHT         *kaddht.IpfsDHT
	MsgNotifier *Notifier
	// TODO: DB client
	StartTime time.Time
}

func NewHost(
	ctx context.Context, 
	ip, port string, 
	bucketSize int, 
	blacklistingUA string, 
	blacklistedPeers map[peer.ID]struct{}) (*Host, error) {
	log.Debug("Creating host")

	// prevent dial backoffs
	ctx = network.WithForceDirectDial(ctx, "prevent backoff")

	// generate private key for the publisher Libp2p host
	privKey, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate priv key for client's host")
	}
	log.Debugf("Generated Priv Key for the host %s", PrivKeyToString(privKey))

	// compose the multiaddress
	mAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", ip, port))
	if err != nil {
		return nil, err
	}

	var dht *kaddht.IpfsDHT
	// generate a new custom message sender
	msgSender := NewCustomMessageSender(blacklistingUA)

	// unlimit the resources for the host 
	limiter := rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits)
	rm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		return nil, fmt.Errorf("new resource manager: %w", err)
	}

	// generate the libp2p host
	h, err := libp2p.New(
		libp2p.WithDialTimeout(DialTimeout),
		libp2p.ListenAddrs(mAddr),
		libp2p.Identity(privKey),
		libp2p.UserAgent(config.UserAgent),
		libp2p.ResourceManager(rm),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			dht, err = kaddht.New(ctx, h,
				kaddht.Mode(kaddht.ModeClient),
				// Consider a Wrapper around MessageSender to get more details of underneath processes
				kaddht.WithCustomMessageSender(msgSender.Init),
				kaddht.BucketSize(bucketSize),
				kaddht.WithPeerBlacklist(blacklistedPeers), // always blacklist (can be an empty map or a full one)
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
		}(*bnode)
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
