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
	GetNewCid() (GetNewCidReturnType, error)
	Type() string
}

//Encapsulates the return type of the GetNewCid()
type GetNewCidReturnType struct {
	ID        peer.ID        `json:"PeerID"`
	CID       cid.Cid        `json:"ContentID"`
	Content   []byte         `json:"Content"`
	Addresses []ma.Multiaddr `json:"PeerMultiaddresses"`
}

func NewGetNewCidReturnType(ID peer.ID, CID cid.Cid, Addresses []ma.Multiaddr) GetNewCidReturnType {
	return GetNewCidReturnType{
		ID:        ID,
		CID:       CID,
		Content:   make([]byte, 0),
		Addresses: Addresses,
	}
}

var Undef = GetNewCidReturnType{}

// JsonFileCIDSource reads CIDs and their content from a json file.
//When you want to access a new CID from the file the GetNewCid() function must be called.
//The Json contents are stored inside the struct and can be accessed with the index
type JsonFileCIDSource struct {
	filename string
	records  ProviderRecords
	index    int
}

//Not yet implemented
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

// NewJsonFileCIDSource Creates a new:
//
// 	type JsonFileCIDSource struct {
//		filename string
//		scanner  *bufio.Scanner
// 	}
//If the file cannot be opened or the data fail to unmarshall it returns the corresponding error.
func NewJsonFileCIDSource(filename string) (*JsonFileCIDSource, error) {
	jsonFile, err := os.Open(filename)
	defer func(jsonFile *os.File) {
		err := jsonFile.Close()
		if err != nil {
			log.Errorf("failed to close file: %s")
		}
	}(jsonFile)
	if err != nil {
		return nil, err
	}

	var records ProviderRecords
	byteValue, err := ioutil.ReadAll(jsonFile)

	if err != nil {
		return nil, errors.Wrap(err, "while trying to read the json file")
	}

	err = json.Unmarshal(byteValue, &records)
	if err != nil {
		return nil, errors.Wrap(err, "while trying to unmarshal json file contents")
	}

	log.Debugf("Read providers and cid from file:%s", string(byteValue))

	return &JsonFileCIDSource{
		filename: filename,
		records:  records,
	}, nil
}

//Not yet implemented
func NewBitswapCIDSource() *BitswapCIDSource {
	return &BitswapCIDSource{}
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

func (g *RandomCidGen) Type() string {
	return "random-content-gen"
}

func (fileCIDSource *JsonFileCIDSource) ResetIndex() {
	fileCIDSource.index = 0
}

//Returns the json records read from the file when creating the file_cid_source instance.
func (file_cid_source *JsonFileCIDSource) GetNewCid() (GetNewCidReturnType, error) {

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
		ProviderAndCidInstance := NewGetNewCidReturnType(newPid, newCid, multiaddr)
		return ProviderAndCidInstance, nil
	}
	file_cid_source.ResetIndex()
	return Undef, nil
}

//TODO type returning a string is not a good idea
func (file_cid_source *JsonFileCIDSource) Type() string {
	return "json-file"
}

func (bitswap_cid_source *BitswapCIDSource) GetNewCid() (GetNewCidReturnType, error) {
	//TODO function that reads bitswap content
	return Undef, nil
}

func (bitswap_cid_source *BitswapCIDSource) Type() string {
	return "bitswap"
}
