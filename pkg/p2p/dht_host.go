package p2p

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	cid "github.com/ipfs/go-cid"
	ma "github.com/multiformats/go-multiaddr"

	libp2p "github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	mplex "github.com/libp2p/go-libp2p-mplex"
	"github.com/libp2p/go-libp2p-xor/key"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	quic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

type ProvideOption string

func GetProvOpFromConf(strOp string) ProvideOption {
	provOp := StandardDHTProvide // Default
	switch strOp {
	case "standard":
		provOp = StandardDHTProvide
	case "optimistic":
		provOp = OpDHTProvide
	}
	return provOp
}

var (
	StandardDHTProvide ProvideOption = "std-dht-provide"
	OpDHTProvide       ProvideOption = "op-provide"

	DefaultUserAgent string = "cid-hoarder"
	PingGraceTime           = 5 * time.Second
	MaxDialAttempts         = 1
	DialTimeout             = 10 * time.Second
)

type DHTHostOptions struct {
	ID               int
	IP               string
	Port             string
	Network          string
	Protocol         protocol.ID
	BootNodes        []peer.AddrInfo
	ProvOp           ProvideOption
	WithNotifier     bool
	K                int
	BlacklistingUA   string
	BlacklistedPeers map[peer.ID]struct{}
}

// DHT Host is the main operational instance to communicate with the IPFS DHT
type DHTHost struct {
	ctx context.Context
	m   sync.RWMutex
	// host related
	opts                DHTHostOptions
	id                  int
	dht                 *kaddht.IpfsDHT
	host                host.Host
	internalMsgNotifier *MsgNotifier
	initTime            time.Time
	// dht query related
	ongoingPings map[cid.Cid]struct{}
}

func NewDHTHost(ctx context.Context, opts DHTHostOptions) (*DHTHost, error) {
	// prevent dial backoffs
	ctx = network.WithForceDirectDial(ctx, "prevent backoff")

	// Libp2p host configuration
	privKey, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate priv key for client's host")
	}
	// transport protocols
	mAddrs := make([]ma.Multiaddr, 0, 2)
	tcpAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s", opts.IP, opts.Port))
	if err != nil {
		return nil, err
	}
	quicAddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%s/quic", opts.IP, opts.Port))
	if err != nil {
		return nil, err
	}
	mAddrs = append(mAddrs, tcpAddr, quicAddr)

	// kad dht options
	msgSender := NewCustomMessageSender(opts.BlacklistingUA, opts.WithNotifier)
	limiter := rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits)
	rm, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		return nil, fmt.Errorf("new resource manager: %w", err)
	}

	// generate the libp2p host
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.UserAgent(DefaultUserAgent),
		libp2p.ListenAddrs(mAddrs...),
		libp2p.WithDialTimeout(DialTimeout),
		libp2p.Security(noise.ID, noise.New),
		libp2p.ResourceManager(rm),
		libp2p.DisableRelay(),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(quic.NewTransport),
		libp2p.Muxer(mplex.ID, mplex.DefaultTransport),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
	)
	if err != nil {
		return nil, err
	}

	// DHT routing
	dhtOpts := []kaddht.Option{
		kaddht.Mode(kaddht.ModeClient),
		kaddht.BootstrapPeers(opts.BootNodes...),
		kaddht.V1ProtocolOverride(opts.Protocol),
		kaddht.WithCustomMessageSender(msgSender.Init),
		kaddht.BucketSize(opts.K),
		kaddht.WithPeerBlacklist(opts.BlacklistedPeers), // always blacklist (can be an empty map or a full one)
	}
	// is there a custom protocol-prefix?
	if opts.ProvOp == OpDHTProvide {
		dhtOpts = append(dhtOpts, kaddht.EnableOptimisticProvide())
	}
	dhtCli, err := kaddht.New(ctx, h, dhtOpts...)
	if err != nil {
		return nil, err
	}

	dhtHost := &DHTHost{
		ctx,
		sync.RWMutex{},
		opts,
		opts.ID,
		dhtCli,
		h,
		msgSender.GetMsgNotifier(),
		time.Now(),
		make(map[cid.Cid]struct{}),
	}

	err = dhtHost.Init()
	if err != nil {
		return nil, errors.Wrap(err, "unable to init host")
	}

	log.WithFields(log.Fields{
		"host-id":    opts.ID,
		"network":    opts.Network,
		"multiaddrs": h.Addrs(),
		"peer_id":    h.ID().String(),
		"protocol":   opts.Protocol,
	}).Info("generated new host")

	return dhtHost, nil
}

// Init makes sure that all the components of the DHT host are successfully initialized
func (h *DHTHost) Init() error {
	return h.bootstrap()
}

func (h *DHTHost) bootstrap() error {
	var succCon int64
	var wg sync.WaitGroup

	hlog := log.WithField("host-id", h.id)
	for _, bnode := range h.opts.BootNodes {
		wg.Add(1)
		go func(bn peer.AddrInfo) {
			defer wg.Done()
			err := h.host.Connect(h.ctx, bn)
			if err != nil {
				hlog.Warn("unable to connect bootstrap node:", bn.String(), err.Error())
			} else {
				hlog.Info("successful connection to bootstrap node:", bn.String())
				atomic.AddInt64(&succCon, 1)
			}
		}(bnode)
	}

	wg.Wait()

	var err error
	if succCon > 0 {
		hlog.Infof("got connected to %d bootstrap nodes", succCon)
	} else {
		return errors.New("unable to connect any of the bootstrap nodes from KDHT")
	}

	// bootstrap the Routing table
	err = h.dht.Bootstrap(h.ctx)
	if err != nil {
		return fmt.Errorf("unable to bootstrap nodes from KDHT. %s", err.Error())
	}

	// force waiting a little bit to let the bootstrap work
	bootstrapTicker := time.NewTicker(5 * time.Second)
	select {
	case <-bootstrapTicker.C:
	case <-h.ctx.Done():
	}

	routingSize := h.dht.RoutingTable().Size()
	if routingSize == 0 {
		return fmt.Errorf("no error, but empty routing table after bootstraping")

	}
	log.WithField(
		"peers_in_routing", routingSize,
	).Info("dht cli bootstrapped")
	return nil
}

func (h *DHTHost) GetHostID() int {
	return h.id
}

func (h *DHTHost) GetMsgNotifier() *MsgNotifier {
	return h.internalMsgNotifier
}

// conside moving this to the Host
func (h *DHTHost) isPeerConnected(pId peer.ID) bool {
	// check if we already have a connection open to the peer
	peerList := h.host.Network().Peers()
	for _, p := range peerList {
		if p == pId {
			return true
		}
	}
	return false
}

func (h *DHTHost) GetUserAgentOfPeer(p peer.ID) (useragent string) {
	userAgentInterf, err := h.host.Peerstore().Get(p, "AgentVersion")
	if err != nil {
		useragent = NoUserAgentDefined
	} else {
		useragent = userAgentInterf.(string)
	}
	return
}

func (h *DHTHost) GetMAddrsOfPeer(p peer.ID) []ma.Multiaddr {
	return h.host.Peerstore().Addrs(p)
}

func (h *DHTHost) ID() peer.ID {
	return h.host.ID()
}

// control the number of CIDs the host is concurrently pinging

func (h *DHTHost) AddCidPing(cidInfo *models.CidInfo) {
	h.m.Lock()
	defer h.m.Unlock()
	h.ongoingPings[cidInfo.CID] = struct{}{}
}

func (h *DHTHost) RemoveCidPing(cidInfo *models.CidInfo) {
	h.m.Lock()
	defer h.m.Unlock()
	delete(h.ongoingPings, cidInfo.CID)
}

func (h *DHTHost) GetOngoingCidPings() int {
	h.m.RLock()
	defer h.m.RUnlock()
	return len(h.ongoingPings)
}

// Deprecater so far
func (h *DHTHost) XORDistanceToOngoingCids(cidHash cid.Cid) (*big.Int, bool) {
	cidK := key.BytesKey([]byte(cidHash.Hash()))
	xorDist := big.NewInt(0)
	if h.GetOngoingCidPings() == 0 {
		return xorDist, true
	}
	h.m.RLock()
	for ongoingCid := range h.ongoingPings {
		onK := key.BytesKey([]byte(ongoingCid.Hash()))
		auxXor := key.DistInt(onK, cidK)
		isNull := xorDist.Cmp(big.NewInt(0))
		isBigger := auxXor.Cmp(xorDist)
		if isNull == int(big.Exact) || isBigger == int(big.Above) {
			xorDist = auxXor
		}
	}
	h.m.RUnlock()
	return xorDist, false
}

// dht pinger related methods

func (h *DHTHost) PingPRHolderOnCid(
	ctx context.Context,
	remotePeer peer.AddrInfo,
	cid *models.CidInfo) *models.PRPingResults {

	hlog := log.WithFields(log.Fields{
		"host-id":   h.id,
		"cid":       cid.CID.Hash().B58String(),
		"pr-holder": remotePeer.ID.String(),
	})

	ctx = network.WithForceDirectDial(ctx, "prevent backoff")
	var active, hasRecords, recordsWithMAddrs bool
	var connError string = DialErrorUnknown
	tstart := time.Now()

	// fulfill the control fields from a successful connection
	succesfulConnection := func() {
		active = true
		connError = NoConnError

		providers, _, err := h.dht.GetProvidersFromPeer(ctx, remotePeer.ID, cid.CID.Hash())
		if err != nil {
			hlog.Debugf("unable to retrieve providers - error: %s", err.Error())
		} else {
			hlog.Debugf("found providers: %v", providers)
		}

		for _, provider := range providers {
			if provider.ID == cid.Creator {
				hasRecords = true
				if len(provider.Addrs) > 0 {
					recordsWithMAddrs = true
				}
			}
		}
		// close the connection to the peer
		err = h.host.Network().ClosePeer(remotePeer.ID)
		if err != nil {
			hlog.Errorf("unable to close connection to peer %s - %s", remotePeer.ID.String(), err.Error())
		}
	}
	// check if the peer is among the already connected ones
	// loop over max tries if the connection is connection refused/ connection reset by peer
connetionRetry:
	for att := 0; att < MaxDialAttempts; att++ {
		// attempt to connect the peer
		err := h.host.Connect(ctx, remotePeer)
		connError = ParseConError(err)
		switch connError {
		case NoConnError: // no error at all
			hlog.Debugf("succesful connection")
			succesfulConnection()
			break connetionRetry

		case DialErrorConnectionRefused, DialErrorStreamReset:
			// the error is due to a connection rejected, try again
			hlog.Debugf("error on connection attempt %s, retrying", err.Error())
			if (att + 1) < MaxDialAttempts {
				ticker := time.NewTicker(PingGraceTime)
				select {
				case <-ticker.C:
					continue
				case <-h.ctx.Done():
					break connetionRetry
				}
			} else {
				hlog.Debugf("error on %d retry %s", att+1, connError)
				break connetionRetry
			}

		default:
			hlog.Debugf("unable to connect - error %s", err.Error())
			break connetionRetry
		}
	}

	return models.NewPRPingResults(
		cid.CID,
		remotePeer.ID,
		-1, // caller will need to update the Round with the given idx
		cid.PublishTime,
		tstart,
		time.Since(tstart),
		active,
		hasRecords,
		recordsWithMAddrs,
		connError)
}

func (h *DHTHost) GetClosestPeersToCid(ctx context.Context, cid *models.CidInfo) (time.Duration, []peer.ID, *kaddht.LookupMetrics, error) {

	startT := time.Now()
	closestPeers, lookupMetrics, err := h.dht.GetClosestPeers(ctx, string(cid.CID.Hash()))
	return time.Since(startT), closestPeers, lookupMetrics, err
}

func (h *DHTHost) ProvideCid(ctx context.Context, cid *models.CidInfo) (time.Duration, *kaddht.LookupMetrics, error) {

	log.WithFields(log.Fields{
		"host-id": h.id,
		"cid":     cid.CID.Hash().B58String(),
	}).Debug("providing cid with", cid.ProvideOp)
	startT := time.Now()
	lookupMetrics, err := h.dht.DetailedProvide(ctx, cid.CID, true)
	return time.Since(startT), lookupMetrics, err
}

func (h *DHTHost) FindXXProvidersOfCID(
	ctx context.Context,
	cid *models.CidInfo,
	targetProviders int) (time.Duration, []peer.AddrInfo, error) {

	log.WithFields(log.Fields{
		"host-id": h.id,
		"cid":     cid.CID.Hash().B58String(),
	}).Debug("looking for providers")
	startT := time.Now()
	providers, _, err := h.dht.LookupForXXProviders(ctx, cid.CID, targetProviders)
	return time.Since(startT), providers, err
}

func (h *DHTHost) FindProvidersOfCID(
	ctx context.Context,
	cid *models.CidInfo) (time.Duration, []peer.AddrInfo, error) {

	log.WithFields(log.Fields{
		"host-id": h.id,
		"cid":     cid.CID.Hash().B58String(),
	}).Debug("looking for providers")
	startT := time.Now()
	providers, err := h.dht.LookupForProviders(ctx, cid.CID)
	return time.Since(startT), providers, err
}

func (h *DHTHost) Close() {
	hlog := log.WithField("host-id", h.id)
	var err error

	err = h.dht.Close()
	if err != nil {
		hlog.Error(errors.Wrap(err, "unable to close DHT client"))
	}

	err = h.host.Close()
	if err != nil {
		hlog.Error(errors.Wrap(err, "unable to close libp2p host"))
	}
	hlog.Info("successfully closed")
}
