package hoarder

import (
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"os"

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

func (g *RandomCidGen) GetNewCid() ([]byte, cid.Cid, error) {
	return genRandomContent(g.contentSize)
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

//Scans the file and reads each line of the file. When it reaches the end of file it returns an EOF error. If another error occurs
//it returns the error. The end of file error means that the file was read successfully.
func (file_cid_source *FileCIDSource) GetNewCid() ([]byte, cid.Cid, error) {

	if file_cid_source.index < len(file_cid_source.records.EncapsulatedJSONProviderRecords) {
		providerRecord := file_cid_source.records.EncapsulatedJSONProviderRecords[file_cid_source.index]
		file_cid_source.index++
		newCid, err := cid.Parse(providerRecord.CID)
		if err != nil {
			return make([]byte, 0), cid.Undef, errors.Wrap(err, " could not parse CID")
		}
		return make([]byte, 0), newCid, nil
	}
	return make([]byte, 0), cid.Undef, errors.New("All the provider records were read from the file")
}

//TODO type returning a string is not a good idea
func (file_cid_source *FileCIDSource) Type() string {
	return "json-file"
}

func (bitswap_cid_source *BitswapCIDSource) GetNewCid() ([]byte, cid.Cid, error) {
	//TODO function that reads bitswap content
	return nil, cid.Undef, nil
}

func (bitswap_cid_source *BitswapCIDSource) Type() string {
	return "bitswap"
}

// TODO: is it worth keeping the content?
// getRandomContent returns generates an array of random bytes with the given size and the composed CID of the content
func genRandomContent(byteLen int) ([]byte, cid.Cid, error) {
	// generate random bytes
	content := make([]byte, byteLen)
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
		return content, cid.Cid{}, errors.Wrap(err, "composing CID")
	}

	log.Infof("generated new CID %s", contID.Hash().B58String())
	return content, contID, nil
}
