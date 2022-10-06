package cid_source

import (
	"github.com/ipfs/go-cid"
	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"ipfs-cid-hoarder/pkg/config"
	"math/rand"
)

type RandomCidGen struct {
	contentSize int
}

func NewRandomCidGen(contentSize int) *RandomCidGen {
	return &RandomCidGen{
		contentSize: contentSize,
	}
}

func (g *RandomCidGen) Type() string {
	return config.RandomContent
}

// TODO: is it worth keeping the content? -> replace with the providers and cid struct
// getRandomContent returns generates an array of random bytes with the given size and the composed CID of the content
func (g *RandomCidGen) GetNewCid() (GetNewCidReturnType, error) {
	// generate random bytes
	content := make([]byte, g.contentSize)
	rand.Read(content)

	//TODO do we have to have different CID types?
	// configure the type of CID that we want
	pref := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}

	// get the CID of the content we just generated
	contID, err := pref.Sum(content)
	if err != nil {
		return Undef, errors.Wrap(err, "composing CID")
	}

	log.Infof("generated new CID %s", contID.Hash().B58String())
	ProvidersAndCidInstance := NewGetNewCidReturnType("", contID, make([]ma.Multiaddr, 0))
	return ProvidersAndCidInstance, nil
}