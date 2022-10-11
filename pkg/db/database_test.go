package db

import (
	"context"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	src "ipfs-cid-hoarder/pkg/cid-source"
	"ipfs-cid-hoarder/pkg/config"
	"ipfs-cid-hoarder/pkg/models"
	"ipfs-cid-hoarder/pkg/p2p"
	"reflect"
	"sync"
	"testing"
	"time"
)

func createDBClient(ctx context.Context, url string) (*DBClient, error) {
	return NewDBClient(ctx, url)
}

func createHost(ctx context.Context) (*p2p.Host, error) {
	// generate private and public keys for the Libp2p host
	priv, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate priv key for client's host")
	}
	log.Debugf("Generated Priv Key for the host %s", p2p.PrivKeyToString(priv))
	// ----- Compose the Publisher or discoverer Libp2p host -----
	pubordishost, err := p2p.NewHost(ctx, priv, config.CliIp, config.CliPort, config.DefaultConfig.K, config.DefaultConfig.HydraFilter)
	if err != nil {
		return nil, errors.Wrap(err, "error generating publisher or discoverer libp2p host for the tool")
	}
	return pubordishost, nil
}

func openJSONFile(ctx context.Context, client *DBClient) error {
	GetNewCidReturnTypeChannel := make(chan *src.GetNewCidReturnType, 10)
	var genWG sync.WaitGroup
	host, err := createHost(ctx)
	if err != nil {
		return errors.Wrap(err, "while creating new host")
	}
	go routineForReceivingFromChannel(ctx, client, host, GetNewCidReturnTypeChannel, &genWG)
	file, err := src.OpenSimpleJSONFile("C:\\Users\\fotis\\GolandProjects\\ipfs-cid-hoarder\\examplejsonfiles\\providers.json")
	if err != nil {
		return errors.Wrap(err, "while trying to open json file")
	}
	for true {
		cid, err := file.GetNewCid()
		if reflect.DeepEqual(cid, src.Undef) {
			break
		}
		if err != nil {
			log.Errorf("error %s while generating new cid", err)
		}
		GetNewCidReturnTypeChannel <- &cid
	}
	close(GetNewCidReturnTypeChannel)
	genWG.Wait()
	return nil
}

func routineForReceivingFromChannel(ctx context.Context, client *DBClient, host *p2p.Host, GetNewCidReturnTypeChannel <-chan *src.GetNewCidReturnType, genWG *sync.WaitGroup) {
	genWG.Add(1)
	defer genWG.Done()
	for {
		select {
		case getNewCidReturnTypeInstance := <-GetNewCidReturnTypeChannel:
			cidInfo := models.NewCidInfo(getNewCidReturnTypeInstance.CID, time.Duration(33), config.JsonFileSource, "json-file", host.ID())
			fetchRes := models.NewCidFetchResults(getNewCidReturnTypeInstance.CID, 0)
			client.AddCidInfo(cidInfo)
			client.AddFetchResult(fetchRes)
			//TODO discoverer starting ping res
		case <-ctx.Done():
			log.Info("shutting down receiving from channel ")
			return
		default:
			//log.Debug("haven't received anything yet")
		}
	}
}

func Test_runPersisters(t *testing.T) {
	ctx := context.Background()
	url := "user=fotis password=docker host=localhost port=5432 sslmode=disable"
	client, err := createDBClient(ctx, url)
	if err != nil {
		t.Errorf("error %s while trying to create database client", err)
	}
	go func() {
		err := openJSONFile(ctx, client)
		if err != nil {
			t.Errorf("error %s while running openjsonfile go routine", err)
		}
	}()
}
