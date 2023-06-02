package p2p

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"

	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	ErrorPoolNotInitialized = errors.New("host pool not initilized")
	ErrorRetrievingBestHost = errors.New("unable to retrieve best suitable host")
)

type HostPool struct {
	m   sync.RWMutex
	ctx context.Context

	hostCnt int

	hostMap   map[peer.ID]*DHTHost
	hostArray []*DHTHost
}

func NewHostPool(ctx context.Context, poolSize int, hOpts DHTHostOptions) (*HostPool, error) {
	hostMap := make(map[peer.ID]*DHTHost)
	hostArray := make([]*DHTHost, 0, poolSize)

	plog := log.WithField("mod", "host-pool")
	plog.Infof("initializing %d hosts", poolSize)

	hostPool := &HostPool{
		sync.RWMutex{},
		ctx,
		0,
		hostMap,
		hostArray,
	}

	var errG errgroup.Group
	for hostId := 0; hostId < poolSize; hostId++ {
		errG.Go(func() error {
			return hostPool.OneMoreHost(hOpts)
		})
	}
	err := errG.Wait()
	if err != nil {
		return nil, err
	}
	plog.WithFields(log.Fields{
		"pool-size": poolSize,
	}).Info("hosts initialized")
	return hostPool, nil
}

func (p *HostPool) OneMoreHost(hOpts DHTHostOptions) error {
	p.m.Lock()
	id := p.hostCnt
	hOpts.ID = id
	p.hostCnt++
	p.m.Unlock()

	h, err := NewDHTHost(p.ctx, hOpts)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("unable to create host %d", hOpts.ID))
	}
	p.m.Lock()
	defer p.m.Unlock()
	p.hostMap[h.ID()] = h
	p.hostArray = append(p.hostArray, h)
	return nil
}

func (p *HostPool) GetBestHost(newCid *models.CidInfo) (*DHTHost, error) {
	p.m.RLock()
	defer p.m.RUnlock()
	xorDists := make([]*big.Int, len(p.hostArray))
	for idx, dhtHost := range p.hostArray {
		xorDist, idle := dhtHost.XORDistanceToOngoingCids(newCid.CID)
		if idle { // if any host is idle, return it directly
			return dhtHost, nil
		}
		xorDists[idx] = xorDist
	}
	// get max dist out of the closes ongoing one
	var hid int
	maxDist := big.NewInt(0)
	for idx, xorDist := range xorDists {
		i := maxDist.Cmp(xorDist)
		if i == int(big.Below) {
			maxDist = xorDist
			hid = idx
		}
	}
	if hid == 0 && maxDist == big.NewInt(0) {
		// return at least the first node in the list (is among the hosts with fewer ongoing cids)
		return p.hostArray[hid], ErrorRetrievingBestHost
	}
	return p.hostArray[hid], nil
}

func (p *HostPool) GetHostWorkload() map[int]int {
	summary := make(map[int]int)
	p.m.RLock()
	defer p.m.RUnlock()

	fmt.Println(p.hostArray)
	for _, host := range p.hostArray {
		summary[host.id] = host.GetOngoingCidPings()
	}
	return summary
}

func (p *HostPool) Close() {
	for _, h := range p.hostArray {
		h.Close()
	}
}

// functions to sort out the host by number of ongoing CID pings
func (p *HostPool) SortHost() {
	sort.Sort(p)
}

func (p *HostPool) Swap(i, j int) {
	p.m.Lock()
	defer p.m.Unlock()

	p.hostArray[i], p.hostArray[j] = p.hostArray[j], p.hostArray[i]
}

func (p *HostPool) Less(i, j int) bool {
	p.m.RLock()
	defer p.m.RUnlock()
	return p.hostArray[i].GetOngoingCidPings() < p.hostArray[j].GetOngoingCidPings()
}

func (p *HostPool) Len() int {
	p.m.RLock()
	defer p.m.RUnlock()
	return len(p.hostArray)
}
