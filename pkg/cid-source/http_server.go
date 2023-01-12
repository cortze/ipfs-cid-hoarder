package cid_source

import (
	"context"
	"encoding/json"
	"fmt"
	"ipfs-cid-hoarder/pkg/config"
	"net/http"
	"reflect"
	"sync"
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
	lock            sync.Mutex
	server          *http.Server
	providerRecords []ProviderRecords
	isStarted       bool
}

func (httpCidSource *HttpCidSource) Dequeue() ProviderRecords {
	if len(httpCidSource.providerRecords) == 0 {
		log.Debug("Queue is empty")
		return ProviderRecords{}
	}
	elem := httpCidSource.providerRecords[0]
	httpCidSource.providerRecords = httpCidSource.providerRecords[1:]
	log.Debugf("Removed element from queue, lenth is now: %d", len(httpCidSource.providerRecords))
	return elem
}

func (httpCidSource *HttpCidSource) Enqueue(providerRecords ProviderRecords) {
	httpCidSource.providerRecords = append(httpCidSource.providerRecords, providerRecords)
	log.Debugf("Added new element to queue, length is now: %d", len(httpCidSource.providerRecords))
}

func NewHttpCidSource(port int, hostname string) *HttpCidSource {
	return &HttpCidSource{
		port:            port,
		hostname:        hostname,
		server:          nil,
		isStarted:       false,
		providerRecords: []ProviderRecords{},
	}
}

func (httpCidSource *HttpCidSource) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Info("Received request in server HTTP")
	if r.URL.Path == "/ProviderRecord" {
		if r.Method == http.MethodPost {
			log.Debug("The request was a post method")
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
			log.Debug("The request was a get request")
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
	}

}

func (httpCidSource *HttpCidSource) StartServer() error {
	log.Info("Starting http server")
	if httpCidSource.isStarted {
		return errors.New("Http server is already started")
	}
	httpCidSource.isStarted = true
	// prepare address
	addr := fmt.Sprintf(":%v", httpCidSource.port)

	httpCidSource.server = &http.Server{
		Addr:    addr,
		Handler: httpCidSource,
	}

	/* http.HandleFunc("/ProviderRecord", func(w http.ResponseWriter, r *http.Request) {
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
	}) */
	if err := httpCidSource.server.ListenAndServe(); err != nil {
		if err == http.ErrServerClosed {
			log.Infof("Server closed under request: %v", err)
		} else {
			log.Fatalf("Server closed unexpect: %v", err)
		}
		httpCidSource.isStarted = false
	}
	return nil
}

func (httpCidSource *HttpCidSource) Shutdown(ctx context.Context) error {
	httpCidSource.lock.Lock()
	defer httpCidSource.lock.Unlock()

	if !httpCidSource.isStarted || httpCidSource.server == nil {
		return errors.New("Server is not started")
	}

	stop := make(chan bool)
	go func() {
		// We can use .Shutdown to gracefully shuts down the server without
		// interrupting any active connection
		httpCidSource.server.Shutdown(ctx)
		stop <- true
	}()

	select {
	case <-ctx.Done():
		log.Errorf("Timeout: %v", ctx.Err())
		break
	case <-stop:
		log.Infof("Finished and shutting down http server")
	}

	return nil
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
			log.Errorf("Error while parsing provide time: %s", err)
		}

		log.Debugf("It's publication time is: %s", providerRecord.PublicationTime)
		newPublicationTime, err := time.Parse(time.RFC3339, providerRecord.PublicationTime)

		if err != nil {
			log.Errorf("Error while parsing publication time: %s", err)
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
		trackableCid := NewTrackableCid(newPid, newCid, newCreator, multiaddresses, newPublicationTime, newProvideTime, providerRecord.UserAgent)
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
