package hoarder

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	ma "github.com/multiformats/go-multiaddr"
	mh "github.com/multiformats/go-multihash"
)

var DefCIDContLen = 1024 // 1KB

type CidSource interface {
	//return pid,multiaddresses and cid
	GetNewCid() (ProviderAndCID, error)
	Type() string
}

type ProviderAndCID struct {
	ID        peer.ID        `json:"PeerID"`
	CID       cid.Cid        `json:"ContentID"`
	Addresses []ma.Multiaddr `json:"PeerMultiaddresses"`
}

func newProvideAndCID(ID peer.ID, CID cid.Cid, Addresses []ma.Multiaddr) ProviderAndCID {
	return ProviderAndCID{
		ID:        ID,
		CID:       CID,
		Addresses: Addresses,
	}
}

var Undef = ProviderAndCID{}

//Read CIDs and their content from a file. The struct will contain the file pointer that it's opened along with a pointer to a scanner
//struct. When you want to access a new CID from the file the GetNewCid() function must be called. The scanner keeps the state and will
//read the file until the end.
type FileCIDSource struct {
	filename string
	jsonFile *os.File
	records  ProviderRecords
	index    int
}

type BitswapCIDSource struct {
}

type RandomCidGen struct {
	contentSize int
}

func NewRandomCidGen(contentSize int) *RandomCidGen {
	return &RandomCidGen{
		contentSize: contentSize,
	}
}

//Creates a new:
//
// 	type FileCIDSource struct {
//		filename string
//		file     *os.File
//		scanner  *bufio.Scanner
// 	}
//If the file cannot be opened it returns the corresponding error.
func newFileCIDSource(filename string) (*FileCIDSource, error) {
	jsonFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var records ProviderRecords
	byteValue, err := ioutil.ReadAll(jsonFile)

	if err != nil {
		return nil, err
	}

	json.Unmarshal(byteValue, &records)

	return &FileCIDSource{
		filename: filename,
		jsonFile: jsonFile, //this a pointer to a file
		records:  records,
	}, nil
}

func newBitswapCIDSource() *BitswapCIDSource {
	return &BitswapCIDSource{}
}

// TODO: is it worth keeping the content? -> replace with the providers and cid struct
// getRandomContent returns generates an array of random bytes with the given size and the composed CID of the content
func (g *RandomCidGen) GetNewCid() (ProviderAndCID, error) {
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
	ProvidersAndCidInstance := newProvideAndCID("", contID, make([]ma.Multiaddr, 0))
	return ProvidersAndCidInstance, nil
}

func (g *RandomCidGen) Type() string {
	return "random-content-gen"
}

func (fileCidSource *FileCIDSource) Close() {
	fileCidSource.jsonFile.Close()
}

func (fileCIDSource *FileCIDSource) ResetIndex() {
	fileCIDSource.index = 0
}

//Returns the json records read from the file when creating the file_cid_source instance.
func (file_cid_source *FileCIDSource) GetNewCid() (ProviderAndCID, error) {

	if file_cid_source.index < len(file_cid_source.records.EncapsulatedJSONProviderRecords) {
		providerRecord := file_cid_source.records.EncapsulatedJSONProviderRecords[file_cid_source.index]
		file_cid_source.index++
		//TODO check if this is right
		newCid, err := cid.Parse(providerRecord.CID)
		if err != nil {
			return Undef, errors.Wrap(err, " could not parse CID")
		}
		newPid, err := peer.IDFromString(providerRecord.ID)
		if err != nil {
			return Undef, errors.Wrap(err, " could not parse PID")
		}
		multiaddr := providerRecord.Address
		log.Infof("Read a new provider ID %s. The multiaddresses are %v.The new CID is %s", newPid, multiaddr, newCid)
		ProviderAndCidInstance := newProvideAndCID(newPid, newCid, multiaddr)
		return ProviderAndCidInstance, nil
	}
	file_cid_source.Close()
	file_cid_source.ResetIndex()
	return Undef, nil
}

//TODO type returning a string is not a good idea
func (file_cid_source *FileCIDSource) Type() string {
	return "json-file"
}

func (bitswap_cid_source *BitswapCIDSource) GetNewCid() (ProviderAndCID, error) {
	//TODO function that reads bitswap content
	return Undef, nil
}

func (bitswap_cid_source *BitswapCIDSource) Type() string {
	return "bitswap"
}
