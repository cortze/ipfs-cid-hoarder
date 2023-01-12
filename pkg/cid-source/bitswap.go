package cid_source

import "github.com/cortze/ipfs-cid-hoarder/pkg/config"

//Not yet implemented
type BitswapCIDSource struct {
}

//Not yet implemented
func NewBitswapCIDSource() *BitswapCIDSource {
	return &BitswapCIDSource{}
}

func (bitswap_cid_source *BitswapCIDSource) Type() string {
	return config.BitswapSource
}

func (bitswap_cid_source *BitswapCIDSource) GetNewCid() (TrackableCid, error) {
	//TODO function that reads bitswap content
	return TrackableCid{}, nil
}
