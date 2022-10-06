package cid_source

import "ipfs-cid-hoarder/pkg/config"

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

func (bitswap_cid_source *BitswapCIDSource) GetNewCid() (GetNewCidReturnType, error) {
	//TODO function that reads bitswap content
	return Undef, nil
}
