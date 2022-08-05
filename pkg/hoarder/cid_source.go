package hoarder

import (
	"bufio"
	"io"
	"math/rand"
	"os"
	"strings"

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
	file     *os.File
	scanner  *bufio.Scanner
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
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrap(err, "opening CID file")
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	log.Info("Creating a new file cid source struct")
	return &FileCIDSource{
		filename: filename,
		file:     file,    //this a pointer to a file
		scanner:  scanner, //this a pointer to the scanner
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

//Scans the file and reads each line of the file. When it reaches the end of file it returns an EOF error. If another error occurs
//it returns the error. The end of file error means that the file was read successfully.
func (file_cid_source *FileCIDSource) GetNewCid() ([]byte, cid.Cid, error) {
	//returns false when the scan stops (reaching the end of the file) or there's an error.
	error_flag := file_cid_source.scanner.Scan()
	if !error_flag {
		//scanner.Err() returns a nil when error_flag = false and the error is an EOF error. Else it return the error normally.
		err := file_cid_source.scanner.Err()
		if err != io.EOF {
			file_cid_source.file.Close()
			return nil, cid.Undef, errors.Wrap(err, " while scanning the file")
		}
		file_cid_source.file.Close()
		return nil, cid.Undef, errors.Wrap(io.EOF, " while scanning the file")
	}
	temp := strings.Fields(file_cid_source.scanner.Text())

	//TODO check if parse function gives the desired functionality
	cid_temp, err := cid.Parse(temp[0])
	if err != nil {
		return nil, cid.Undef, errors.Wrap(err, " while parsing cid")
	}
	//for now we only read the CIDs and not their contents
	log.Infof("Read new CID from file %s", cid_temp)
	return nil, cid_temp, nil
}

func (file_cid_source *FileCIDSource) Type() string {
	return "text-file"
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
