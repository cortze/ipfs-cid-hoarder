package cid_source

import (
	"encoding/json"
	"fmt"
	"ipfs-cid-hoarder/pkg/config"
	"net/http"
)

type HttpCidSource struct {
	port          int
	hostname      string
	trackableCids []TrackableCid
	lastIndex     int
}

func (httpCidSource *HttpCidSource) StartServer() {
	http.HandleFunc("/ProviderRecord", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			// create a new TrackableCid instance
			var trackableCid TrackableCid

			// decode the request body into the TrackableCid struct
			err := json.NewDecoder(r.Body).Decode(&trackableCid)
			if err != nil {
				http.Error(w, "Error decoding request body", http.StatusBadRequest)
				return
			}
			// add the trackableCid to the list
			httpCidSource.trackableCids = append(httpCidSource.trackableCids, trackableCid)
		} else if r.Method == http.MethodGet {
			// check if there are any trackableCids to return
			if httpCidSource.lastIndex >= len(httpCidSource.trackableCids) {
				http.Error(w, "No more trackableCids available", http.StatusNoContent)
				return
			}
			// return the last unretrieved trackableCid
			trackableCid := httpCidSource.trackableCids[httpCidSource.lastIndex]
			// send the trackableCid back to the client as a response
			json.NewEncoder(w).Encode(trackableCid)
			httpCidSource.lastIndex++
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
		return TrackableCid{}, err
	}
	defer resp.Body.Close()

	// check the status code
	if resp.StatusCode == http.StatusNoContent {
		return TrackableCid{}, fmt.Errorf("No more trackableCids available")
	} else if resp.StatusCode != http.StatusOK {
		return TrackableCid{}, fmt.Errorf("Error retrieving trackableCid: %s", resp.Status)
	}

	// decode the response into a TrackableCid struct
	var trackableCid TrackableCid
	err = json.NewDecoder(resp.Body).Decode(&trackableCid)
	if err != nil {
		return TrackableCid{}, err
	}

	return trackableCid, nil

}

func (HttpCidSource *HttpCidSource) Type() string {
	return config.HttpServerSource
}
