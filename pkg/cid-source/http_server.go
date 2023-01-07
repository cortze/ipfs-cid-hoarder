package cid_source

import (
	"encoding/json"
	"fmt"
	"ipfs-cid-hoarder/pkg/config"
	"net/http"
	"reflect"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type HttpCidSource struct {
	port                            int
	hostname                        string
	encapsulatedJSONProviderRecords []EncapsulatedJSONProviderRecord
}

func (httpCidSource *HttpCidSource) pop() EncapsulatedJSONProviderRecord {
	if len(httpCidSource.encapsulatedJSONProviderRecords) == 0 {
		log.Debug("Stack is empty")
		return EncapsulatedJSONProviderRecord{}
	}
	length := len(httpCidSource.encapsulatedJSONProviderRecords)
	elem := httpCidSource.encapsulatedJSONProviderRecords[length-1]
	httpCidSource.encapsulatedJSONProviderRecords = httpCidSource.encapsulatedJSONProviderRecords[:length-1]
	return elem
}

func (httpCidSource *HttpCidSource) push(encapsulatedJSONProviderRecord EncapsulatedJSONProviderRecord) {
	httpCidSource.encapsulatedJSONProviderRecords = append(httpCidSource.encapsulatedJSONProviderRecords, encapsulatedJSONProviderRecord)
}

func NewHttpCidSource(port int, hostname string) *HttpCidSource {
	return &HttpCidSource{
		port:                            port,
		hostname:                        hostname,
		encapsulatedJSONProviderRecords: []EncapsulatedJSONProviderRecord{},
	}
}

func (httpCidSource *HttpCidSource) StartServer() {
	http.HandleFunc("/ProviderRecord", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			// create a new TrackableCid instance
			var encapsulatedJSONProviderRecord EncapsulatedJSONProviderRecord

			// decode the request body into the TrackableCid struct
			err := json.NewDecoder(r.Body).Decode(&encapsulatedJSONProviderRecord)
			if err == nil {
				log.Info("Decoded new encapsulated json received from post")
				log.Infof("Read a new provider ID %s.The multiaddresses are %v. The creator is %s. The new CID is %s", string(encapsulatedJSONProviderRecord.ID),
					encapsulatedJSONProviderRecord.Addresses, encapsulatedJSONProviderRecord.Creator, encapsulatedJSONProviderRecord.CID)

				// add the trackableCid to the list
				httpCidSource.encapsulatedJSONProviderRecords = append(httpCidSource.encapsulatedJSONProviderRecords, encapsulatedJSONProviderRecord)
			} else {
				/* fmt.Println("Decoded new trackable cid received from post")
				fmt.Printf("Read a new provider ID %s.The multiaddresses are %v. The creator is %s. The new CID is %s", string(encapsulatedJSONProviderRecord.ID),
					encapsulatedJSONProviderRecord.Addresses, encapsulatedJSONProviderRecord.Creator, encapsulatedJSONProviderRecord.CID) */

				http.Error(w, "Error decoding request body", http.StatusBadRequest)
			}

		} else if r.Method == http.MethodGet {
			// check if there are any trackableCids to return
			if len(httpCidSource.encapsulatedJSONProviderRecords) != 0 {
				// return the last unretrieved trackableCid
				encapsulatedJSONProviderRecord := httpCidSource.pop()
				log.Info("Sending new encapsulated json cid to user with get method")
				// send the trackableCid back to the client as a response
				json.NewEncoder(w).Encode(encapsulatedJSONProviderRecord)
			} else {

				http.Error(w, "No record available currently", http.StatusNoContent)
			}

		} else {
			// return "Method Not Allowed" error
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})
	http.ListenAndServe(":8080", nil)
}

func (httpCidSource *HttpCidSource) GetNewCid() (TrackableCid, error) {
	// send a GET request to the server
	url := fmt.Sprintf("http://%s:%d/ProviderRecord", httpCidSource.hostname, httpCidSource.port)
	resp, err := http.Get(url)
	if err != nil {
		return TrackableCid{ID: "dummy"}, err
	}
	defer resp.Body.Close()

	// check the status code
	if resp.StatusCode == http.StatusNoContent {
		return TrackableCid{ID: "dummy"}, errors.New(fmt.Sprintf("Error while retrieving new cid from stack: %s", resp.Status))
	} else if resp.StatusCode != http.StatusOK {
		return TrackableCid{ID: "dummy"}, errors.New(fmt.Sprintf("Error retrieving trackableCid: %s", resp.Status))
	}

	// decode the response into a EncapsulatedJSONProviderRecord struct
	var pr EncapsulatedJSONProviderRecord
	err = json.NewDecoder(resp.Body).Decode(&pr)
	if err != nil {
		return TrackableCid{ID: "dummy"}, errors.Wrap(err, " while decoding trackable cid")
	}

	if reflect.DeepEqual(pr, EncapsulatedJSONProviderRecord{}) {
		return TrackableCid{}, nil
	}

	log.Debug("Read a new PR from the web server:")

	log.Debugf("It's cid is: %s", pr.CID)
	newCid, err := cid.Parse(pr.CID)
	if err != nil {
		log.Errorf("could not convert string to cid %s", err)
	}

	log.Debugf("It's peer id is: %s", pr.ID)
	newPid, err := peer.Decode(pr.ID)
	if err != nil {
		log.Errorf("could not convert string to pid %s", err)
	}

	log.Debugf("It's creator is: %s", pr.Creator)
	newCreator, err := peer.Decode(pr.Creator)
	if err != nil {
		log.Errorf("could not convert string to creator pid %s", err)
	}

	log.Debugf("It's provide time is: %s", pr.ProvideTime)
	newProvideTime, err := time.ParseDuration(pr.ProvideTime)

	if err != nil {
		log.Errorf("Error while parsing time: %s", err)
	}

	log.Debugf("It's user agent is: %s", pr.UserAgent)

	multiaddresses := make([]ma.Multiaddr, 0)
	for i := 0; i < len(pr.Addresses); i++ {
		multiaddr, err := ma.NewMultiaddr(pr.Addresses[i])
		if err != nil {
			//log.Errorf("could not convert string to multiaddress %s", err)
			continue
		}
		multiaddresses = append(multiaddresses, multiaddr)
	}

	log.Infof("generated new CID %s", newCid.Hash().B58String())

	log.Infof("Read a new provider ID %s.The multiaddresses are %v. The creator is %s. The new CID is %s", string(newPid), multiaddresses, newCreator, newCid)
	trackableCid := NewTrackableCid(newPid, newCid, newCreator, multiaddresses, newProvideTime, pr.UserAgent)

	return trackableCid, nil

}

func (HttpCidSource *HttpCidSource) Type() string {
	return config.HttpServerSource
}
