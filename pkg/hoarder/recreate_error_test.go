package hoarder

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"

	cid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestRecreateRetrievavilityError(t *testing.T) {

	// 1. CIDs
	// RFM 19 Report's CID = "bafybeigdjyzepom74haqau7ojzay7zgwula4kss6nxoyqmrrrwwzjphf5q"
	cidStrB58 := "QmaniDqoEobVH1TpUf9gfa8eE3F9TSr4XM8VY4oqzcwJYw" // "bafkreihbqpjpybzekyqjtyi5wibkgggvo335poijs5u5adhpbxhidby5va"
	c, err := cid.Decode(cidStrB58)
	require.NoError(t, err)
	fmt.Println("CID = ", c.Hash().B58String())

	var peers []string = []string{
		"/ip4/3.145.99.95/tcp/30005/p2p/12D3KooWPRJBihUYXMNfbWiH7L2scuBxsVGwcyCHCAfqm5cxRPhR",
		"/ip4/18.221.158.26/tcp/30009/p2p/12D3KooWH97b1e6XvxraXLKvAqnKFNWyqJzCNauBkiMT6Cz7BvJy",
		"/ip4/18.218.153.41/tcp/30005/p2p/12D3KooWP9ZCUR7D9jmjT1KrvYJ1Nm9pb2SRG8CjvWqveBR2BrsP"}

	// compose string to multiaddres for each peer
	var pAddrss []ma.Multiaddr

	for _, p := range peers {
		madd, err := ma.NewMultiaddr(p)
		require.NoError(t, err)
		pAddrss = append(pAddrss, madd)
	}

	// Compose AddrIngo for each multiaddress
	prHolders, err := peer.AddrInfosFromP2pAddrs(pAddrss...)
	require.NoError(t, err)

	// 2. Compose DHT and host to perform the searches

	// generate private and public keys for the Libp2p host
	priv, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	require.NoError(t, err)

	// ----- Compose the Libp2p host -----
	h, err := p2p.NewHost(context.Background(), priv, "0.0.0.0", "9001", 20, false)
	require.NoError(t, err)
	err = h.Boostrap(context.Background())

	// perform both calls concurrently
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		providers, err := h.DHT.LookupForProviders(context.Background(), c)
		require.NoError(t, err)
		fmt.Println("providers from look up (", len(providers), ") =", providers)
	}()

	for _, pHold := range prHolders {
		wg.Add(1)
		go func(ctx context.Context, pAddr peer.AddrInfo) {
			defer wg.Done()

			// connect the peer
			pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()

			// loop over max tries if the connection is connection refused/ connection reset by peer
			for att := 0; att < 2; att++ {
				// TODO: attempt at least to see if the connection refused
				err := h.Connect(pingCtx, pAddr)
				if err != nil {
					fmt.Printf("unable to connect peer %s - error %s", pAddr.ID.String(), err.Error())
					connError := p2p.ParseConError(err)
					// If the error is not linked to an connection refused or reset by peer, just break the look
					if connError != p2p.DialErrorConnectionRefused && connError != p2p.DialErrorStreamReset {
						break
					}
				} else {
					fmt.Printf("succesful connection to peer %s\n", pAddr.ID.String())

					// if the connection was successfull, request wheter it has the records or not
					provs, _, err := h.DHT.GetProvidersFromPeer(ctx, pAddr.ID, c.Hash())
					if err != nil {
						fmt.Printf("unable to retrieve providers from peer %s - error: %s\n", pAddr.ID, err.Error())
					} else {
						fmt.Printf("providers from peer %s - %v\n", pAddr.ID.String(), provs)
					}

					// close connection and exit loop
					err = h.Network().ClosePeer(pAddr.ID)
					require.NoError(t, err)
					break
				}
			}
		}(context.Background(), pHold)
	}

	wg.Wait()

}
