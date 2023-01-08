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
	port            int
	hostname        string
	providerRecords []ProviderRecords
	done            EncapsulatedJSONProviderRecord
}

func (httpCidSource *HttpCidSource) Dequeue() ProviderRecords {
	if len(httpCidSource.providerRecords) == 0 {
		log.Debug("Queue is empty")
		return ProviderRecords{}
	}
	elem := httpCidSource.providerRecords[0]
	httpCidSource.providerRecords = httpCidSource.providerRecords[1:]
	return elem
}

func (httpCidSource *HttpCidSource) Enqueue(providerRecords ProviderRecords) {
	httpCidSource.providerRecords = append(httpCidSource.providerRecords, providerRecords)
}

func NewHttpCidSource(port int, hostname string) *HttpCidSource {
	return &HttpCidSource{
		port:            port,
		hostname:        hostname,
		providerRecords: []ProviderRecords{},
	}
}

func (httpCidSource *HttpCidSource) StartServer() {
	log.Info("Starting http server")
	http.HandleFunc("/ProviderRecord", func(w http.ResponseWriter, r *http.Request) {
		log.Info("Set handler for requests")
		if r.Method == http.MethodPost {
			// create a new TrackableCid instance
			var providerRecords ProviderRecords

			// decode the request body into the TrackableCid struct
			err := json.NewDecoder(r.Body).Decode(&providerRecords)
			if err == nil {
				log.Info("Decoded new encapsulated json received from post")

				// add the trackableCid to the list
				httpCidSource.Enqueue(providerRecords)
			} else {

				http.Error(w, "Error decoding request body", http.StatusBadRequest)
			}

		} else if r.Method == http.MethodGet {
			// check if there are any trackableCids to return
			if len(httpCidSource.providerRecords) != 0 {
				// return the last unretrieved trackableCid
				providerRecords := httpCidSource.Dequeue()
				log.Info("Sending new encapsulated json cid to user with get method")
				// send the trackableCid back to the client as a response
				json.NewEncoder(w).Encode(providerRecords)
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

func (HttpCidSource *HttpCidSource) shutdown() {
	http.HandleFunc("/ProviderRecord", nil)
	http.ListenAndServe("", nil)
}

func GetNewHttpCid(source interface{}) ([]TrackableCid, error) {

	httpCidSource, ok := source.(*HttpCidSource)
	if !ok {
		return []TrackableCid{}, fmt.Errorf("Invalid source type: %T", source)
	}

	// send a GET request to the server
	url := fmt.Sprintf("http://%s:%d/ProviderRecord", httpCidSource.hostname, httpCidSource.port)
	resp, err := http.Get(url)
	if err != nil {
		return []TrackableCid{}, err
	}
	defer resp.Body.Close()

	// check the status code
	if resp.StatusCode == http.StatusNoContent {
		return []TrackableCid{}, errors.New(fmt.Sprintf("Error while retrieving new cid from stack: %s", resp.Status))
	} else if resp.StatusCode != http.StatusOK {
		return []TrackableCid{}, errors.New(fmt.Sprintf("Error retrieving trackableCid: %s", resp.Status))
	}

	// decode the response into a EncapsulatedJSONProviderRecord struct
	var providerRecords ProviderRecords
	err = json.NewDecoder(resp.Body).Decode(&providerRecords)
	if err != nil {
		return []TrackableCid{}, errors.Wrap(err, " while decoding trackable cid")
	}

	if reflect.DeepEqual(providerRecords, ProviderRecords{}) {
		return nil, errors.New("ended providing")
	}

	var trackableCidPrs []TrackableCid

	for _, providerRecord := range providerRecords.EncapsulatedJSONProviderRecords {
		log.Debug("Read a new PR from the web server:")

		log.Debugf("It's cid is: %s", providerRecord.CID)
		newCid, err := cid.Parse(providerRecord.CID)
		if err != nil {
			log.Errorf("could not convert string to cid %s", err)
		}

		log.Debugf("It's peer id is: %s", providerRecord.ID)
		newPid, err := peer.Decode(providerRecord.ID)
		if err != nil {
			log.Errorf("could not convert string to pid %s", err)
		}

		log.Debugf("It's creator is: %s", providerRecord.Creator)
		newCreator, err := peer.Decode(providerRecord.Creator)
		if err != nil {
			log.Errorf("could not convert string to creator pid %s", err)
		}

		log.Debugf("It's provide time is: %s", providerRecord.ProvideTime)
		newProvideTime, err := time.ParseDuration(providerRecord.ProvideTime)

		if err != nil {
			log.Errorf("Error while parsing time: %s", err)
		}

		log.Debugf("It's user agent is: %s", providerRecord.UserAgent)

		multiaddresses := make([]ma.Multiaddr, 0)
		for i := 0; i < len(providerRecord.Addresses); i++ {
			multiaddr, err := ma.NewMultiaddr(providerRecord.Addresses[i])
			if err != nil {
				//log.Errorf("could not convert string to multiaddress %s", err)
				continue
			}
			multiaddresses = append(multiaddresses, multiaddr)
		}

		log.Infof("generated new CID %s", newCid.Hash().B58String())

		log.Infof("Read a new provider ID %s.The multiaddresses are %v. The creator is %s. The new CID is %s", string(newPid), multiaddresses, newCreator, newCid)
		trackableCid := NewTrackableCid(newPid, newCid, newCreator, multiaddresses, newProvideTime, providerRecord.UserAgent)
		trackableCidPrs = append(trackableCidPrs, trackableCid)
	}

	return trackableCidPrs, nil

}

func (httpCidSource *HttpCidSource) GetNewCid() (TrackableCid, error) {
	return TrackableCid{}, nil
}

func (httpCidSource *HttpCidSource) Type() string {
	return config.HttpServerSource
}
