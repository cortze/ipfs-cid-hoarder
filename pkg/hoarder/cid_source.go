package hoarder

import (
	"math/rand"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	mh "github.com/multiformats/go-multihash"
)

var DefCIDContLen = 1024 // 1KB

type CidSource interface {
	GetNewCid() ([]byte, cid.Cid, error)
	Type() string
}

//Read CIDs and their content(?) from a file
type File_CID_Source struct {
}

type Bitswap_CID_Source struct {
}

type RandomCidGen struct {
	contentSize int
}

func NewRandomCidGen(contentSize int) *RandomCidGen {
	return &RandomCidGen{
		contentSize: contentSize,
	}
}

func newFileCIDSource() *File_CID_Source {
	return &File_CID_Source{}
}

func newBitswapCIDSource() *Bitswap_CID_Source {
	return &Bitswap_CID_Source{}
}

func (g *RandomCidGen) GetNewCid() ([]byte, cid.Cid, error) {
	return genRandomContent(g.contentSize)
}

func (g *RandomCidGen) Type() string {
	return "random-content-gen"
}

func (file_cid_source *File_CID_Source) GetNewCid() ([]byte, cid.Cid, error) {
	return read_content_from_file()
}

func (file_cid_source *File_CID_Source) Type() string {
	return "text-file"
}

func (bitswap_cid_source *Bitswap_CID_Source) GetNewCid() ([]byte, cid.Cid, error) {
	//TODO function that reads bitswap content
	return read_content_from_file()
}

func (bitswap_cid_source *Bitswap_CID_Source) Type() string {
	return "bitswap"
}

func read_content_from_file() ([]byte, cid.Cid, error) {

}

// TODO: is it worth keeping the content?
// getRandomContent returns generates an array of random bytes with the given size and the composed CID of the content
func genRandomContent(byteLen int) ([]byte, cid.Cid, error) {
	// generate random bytes
	content := make([]byte, byteLen)
	rand.Read(content)

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
		return content, cid.Cid{}, errors.Wrap(err, "composing CID")
	}

	log.Infof("generated new CID %s", contID.Hash().B58String())
	return content, contID, nil
}
