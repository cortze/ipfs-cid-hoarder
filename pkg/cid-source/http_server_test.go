package cid_source

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"ipfs-cid-hoarder/pkg/config"
	"ipfs-cid-hoarder/pkg/models"
	"ipfs-cid-hoarder/pkg/p2p"
	"net/http"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	log "github.com/sirupsen/logrus"
)

func PostRequestProviders() {
	records1 := ProviderRecords{
		EncapsulatedJSONProviderRecords: []EncapsulatedJSONProviderRecord{},
	}

	records2 := ProviderRecords{
		EncapsulatedJSONProviderRecords: []EncapsulatedJSONProviderRecord{},
	}

	records1.EncapsulatedJSONProviderRecords = append(records1.EncapsulatedJSONProviderRecords,
		NewEncapsulatedJSONCidProvider(
			"12D3KooWMqsgdFjkn9gdnRBmHGmn8M82rc2xEg6kyVJT3MyinV2y",
			"QmcAVd2AZPBvG6XKZVGsSkwdkDimME4e9cTn7QR1KN4Edv",
			[]string{"/ip6/2602:ff16:6:0:1:1c1:0:1/tcp/4001", "/ip6/::1/tcp/4001", "/ip4/127.0.0.1/udp/4001/quic",
				"/ip6/::1/udp/4001/quic", "/ip6/2602:ff16:6:0:1:1c1:0:1/udp/4001/quic", "/ip4/89.233.108.3/udp/4001/quic",
				"/ip4/127.0.0.1/tcp/4001", "/ip4/89.233.108.3/tcp/4001"},
			"QmWCmp2w4MVvuWSwfYJyzDBNJxmub5mccYsSEhmKMq1zfW",
			"2023-01-16T14:04:42+02:00",
			"0s",
			"go-ipfs/0.7.0/",
		),
		NewEncapsulatedJSONCidProvider(
			"12D3KooWDaG92o6WsCio9KNEgtwMH5uYQ7CrjinWoTp7E7mwYSj8",
			"QmcAVd2AZPBvG6XKZVGsSkwdkDimME4e9cTn7QR1KN4Edv",
			[]string{"/ip6/2a01:4f9:c010:d4d4::1/tcp/4001", "/ip6/64:ff9b::4115:3f3e/udp/4001/quic",
				"/ip6/2a01:4f9:c010:d4d4::1/udp/4001/quic", "/ip6/::1/udp/4001/quic", "/ip4/65.21.63.62/udp/4001/quic",
				"/ip4/65.21.63.62/tcp/4001", "/ip4/127.0.0.1/tcp/4001", "/ip6/::1/tcp/4001", "/ip4/127.0.0.1/udp/4001/quic"},
			"QmWCmp2w4MVvuWSwfYJyzDBNJxmub5mccYsSEhmKMq1zfW",
			"2023-01-16T14:04:42+02:00",
			"0s",
			"go-ipfs/0.8.0/",
		),
	)

	records2.EncapsulatedJSONProviderRecords = append(records2.EncapsulatedJSONProviderRecords,
		NewEncapsulatedJSONCidProvider(
			"12D3KooWCqCptb37u82qWDtrkWQH648Hcbh7McZXwNTaT8s4oFpH",
			"QmRpBb76FipRtYD9TVqJyyweURozccUpRCXSvFWfVuSu8U",
			[]string{"/ip4/45.63.7.28/tcp/4001", "/ip4/127.0.0.1/udp/4001/quic",
				"/ip4/45.63.7.28/udp/4001/quic", "/ip6/::1/udp/4001/quic", "/ip6/::1/tcp/4001", "/ip4/127.0.0.1/tcp/4001"},
			"QmWCmp2w4MVvuWSwfYJyzDBNJxmub5mccYsSEhmKMq1zfW",
			"2023-01-16T14:04:42+02:00",
			"0s",
			"kubo/0.14.0/e0fabd6",
		),
		NewEncapsulatedJSONCidProvider(
			"12D3KooWMwDswsL9c4Fa3c6UtgxvGxNoPBJ5dYCp8kzrXmJe59xP",
			"QmRpBb76FipRtYD9TVqJyyweURozccUpRCXSvFWfVuSu8U",
			[]string{"/ip4/127.0.0.1/udp/4001/quic", "/ip4/165.227.164.94/tcp/4001",
				"/ip6/64:ff9b::a5e3:a45e/udp/4001/quic", "/ip6/::1/udp/4001/quic", "/ip4/127.0.0.1/tcp/4001", "/ip6/::1/tcp/4001", "/ip4/165.227.164.94/udp/4001/quic"},
			"QmWCmp2w4MVvuWSwfYJyzDBNJxmub5mccYsSEhmKMq1zfW",
			"2023-01-16T14:04:42+02:00",
			"0s",
			"go-ipfs/0.7.0/",
		),
	)

	// create a POST request
	data, err := json.Marshal(records1)
	if err != nil {
		log.Errorf("Error marshalling provider records for cid: %s", err)
	}
	req, err := http.NewRequest("POST", "http://localhost:8080/", bytes.NewReader(data))
	if err != nil {
		log.Errorf("Error creating POST request: %s", err)
	}
	// send the post request
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("Error sending POST request: %s", err)
	}

	// create a POST request
	data, err = json.Marshal(records2)
	if err != nil {
		log.Errorf("Error marshalling provider records for cid: %s", err)
	}
	req, err = http.NewRequest("POST", "http://localhost:8080/", bytes.NewReader(data))
	if err != nil {
		log.Errorf("Error creating POST request: %s", err)
	}
	// send the post request
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("Error sending POST request: %s", err)
	}

	// create a POST request
	data, err = json.Marshal(nil)
	if err != nil {
		log.Errorf("Error marshalling provider records for cid: %s", err)
	}
	req, err = http.NewRequest("POST", "http://localhost:8080/", bytes.NewReader(data))
	if err != nil {
		log.Errorf("Error creating POST request: %s", err)
	}
	// send the post request
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("Error sending POST request: %s", err)
	}

	/* // create a POST request
	data, err = json.Marshal(nil)
	if err != nil {
		log.Errorf("Error marshalling provider records for cid: %s", err)
	}
	req, err = http.NewRequest("POST", "http://localhost:8080/", bytes.NewReader(data))
	if err != nil {
		log.Errorf("Error creating POST request: %s", err)
	}
	// send the post request
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Errorf("Error sending POST request: %s", err)
	} */

}

func GetCidFromChannel(httpSource *HttpCidSource) error {

	trackableCidsChannel := make(chan []TrackableCid, 10)

	go GetNewHttpCid(httpSource, trackableCidsChannel)
	counter := 0
	for {
		select {
		case trackableCids, ok := <-trackableCidsChannel:
			if !ok {
				log.Debug("Received not ok message from channel")
				return errors.New("Received not ok message from channel")
			}
			if trackableCids == nil {
				log.Debug("Received nil trackable CIDs from channel")
				return errors.New("Received nil trackable CIDs from channel")
			}

			tr := trackableCids[0]
			cidStr := tr.CID.Hash().B58String()

			log.Debugf(
				"New trackable CID array received from http channel. Cid:%s,ProvideTime:%s,PublicationTime:%s,Creator:%s. It's number is %d",
				cidStr, tr.ProvideTime, tr.PublicationTime, tr.Creator, counter,
			)
			counter++

			//the starting values for the discoverer
			cidIn, err := cid.Parse(cidStr)

			if err != nil {
				log.Errorf("couldnt parse cid")
			}
			//dummy values for test
			pingInterval, _ := time.ParseDuration("30m")

			studyDuration, _ := time.ParseDuration("24h")

			cidInfo := models.NewCidInfo(cidIn, pingInterval, studyDuration, config.JsonFileSource,
				"http-server", "")

			cidInfo.AddPublicationTime(tr.PublicationTime)
			cidInfo.AddProvideTime(tr.ProvideTime)
			cidInfo.AddCreator(tr.Creator)

			fetchRes := models.NewCidFetchResults(cidIn, 0)

			// generate a new CidFetchResults
			//TODO starting data for the discoverer
			fetchRes.TotalHops = 0
			fetchRes.HopsToClosest = 0
			for _, trackableCid := range trackableCids {
				log.Debugf(
					"For looping the trackable CID array for trackable CID: %d. The peer ID is: %s. The peer Multiaddresses are: %v. The user agent is: %s ",
					counter-1, trackableCid.ID.String(), trackableCid.Addresses, trackableCid.UserAgent)
				/* err := addPeerToProviderStore(ctx, discoverer.host, trackableCid.ID, trackableCid.CID, trackableCid.Addresses)
				if err != nil {
					log.Errorf("error %s calling addpeertoproviderstore method", err)
				} else {
					log.Debug("Added providers to provider store")
				}

				err = addAgentVersionToProvideStore(discoverer.host, trackableCid.ID, trackableCid.UserAgent)

				if err != nil {
					log.Errorf("error %s calling addAgentVersionToProvideStore", err)
				} else {
					log.Debug("Added agent version to provider store")
				} */

				//TODO discoverer starting ping res
				pingRes := models.NewPRPingResults(
					cidIn,
					trackableCid.ID,
					//the below are starting data for the discoverer
					0,
					time.Time{},
					0,
					true,
					true,
					p2p.NoConnError,
				)

				/* log.Debugf("User agent received from provider store: %s", discoverer.host.GetUserAgentOfPeer(trackableCid.ID)) */

				prHolderInfo := models.NewPeerInfo(
					trackableCid.ID,
					trackableCid.Addresses,
					trackableCid.UserAgent,
				)

				cidInfo.AddPRHolder(prHolderInfo)
				fetchRes.AddPRPingResults(pingRes)

			}
			cidInfo.AddPRFetchResults(fetchRes)

			tot, success, failed := cidInfo.GetFetchResultSummaryOfRound(0)
			if tot < 0 {
				log.Warnf("no ping results for the PR provide round of Cid %s", cidInfo.CID.Hash().B58String())
			} else {
				log.Infof("Cid %s - %d total PRHolders | %d successfull PRHolders | %d failed PRHolders",
					cidIn, tot, success, failed)
			}
		default:
			//log.Debug("haven't received anything yet")
		}
	}

	return nil
}

func TestGetRequest(t *testing.T) {

	httpSource := NewHttpCidSource(8080, "localhost")
	go httpSource.StartServer()

	PostRequestProviders()
	_ = GetCidFromChannel(httpSource)
	/* if err != nil {
		t.Errorf("%s", err)
	} */
	httpSource.Shutdown(context.TODO())
}
