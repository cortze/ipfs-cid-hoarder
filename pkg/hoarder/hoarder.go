package hoarder

import (
	"context"
	"sync"
	"time"

	src "github.com/cortze/ipfs-cid-hoarder/pkg/cid-source"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/db"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
)

type CidHoarder struct {
	ctx context.Context
	wg  *sync.WaitGroup

	Host       *p2p.Host
	PingerHost *p2p.Host
	DBCli      *db.DBClient
	CidTracker Tracker
	CidPinger  *CidPinger
}

func NewCidHoarder(ctx context.Context, conf *config.Config) (*CidHoarder, error) {
	var err error

	// ----- Compose the DB client -----
	dbInstance, err := db.NewDBClient(ctx, conf.Database)
	if err != nil {
		return nil, errors.Wrap(err, "initialise the DB")
	}

	// generate private key for the publisher Libp2p host
	pubprivk, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate priv key for client's host")
	}
	log.Debugf("Generated Priv Key for the host %s", p2p.PrivKeyToString(pubprivk))

	// generate new priv key for pinger host
	pingprivk, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate priv key for pinger's host")
	}

	// prevent dial backoffs
	ctx = network.WithForceDirectDial(ctx, "prevent backoff")

	// ----- Compose the Publisher or discoverer Libp2p host -----
	pubordishost, err := p2p.NewHost(ctx, pubprivk, config.CliIp, conf.Port, conf.K, conf.HydraFilter)
	if err != nil {
		return nil, errors.Wrap(err, "error generating publisher or discoverer libp2p host for the tool")
	}

	// ----- Compose Pinger Libp2p Host -----
	pingerHost, err := p2p.NewHost(ctx, pingprivk, config.CliIp, conf.Port, conf.K, conf.HydraFilter)
	if err != nil {
		return nil, errors.Wrap(err, "error generating pinger libp2p host for the tool")
	}

	//  ------ Study Parameters ---------
	var studyWG sync.WaitGroup

	reqInterval, err := time.ParseDuration(conf.ReqInterval)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing ReqInterval "+conf.ReqInterval)
	}
	studyDuration, err := time.ParseDuration(conf.StudyDuration)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing StudyDuration"+conf.StudyDuration)
	}

	log.Info("configured Hoarder to request at an interval of ", reqInterval, " and during ", studyDuration)

	// TODO: understimate the number of workers that will be needed might add certain delay between rounds
	//       Better to swap it to check the final time for finishing the run than counting rounds
	rounds := int(studyDuration/reqInterval) + 1

	// ----- Generate the CidPinger -----
	studyWG.Add(1)
	cidPinger := NewCidPinger(ctx, &studyWG, pingerHost, dbInstance, reqInterval, rounds, conf.Workers)

	//TODO this needs to be changed meaning the generation
	// ----- Generate the CidTracker(either a discoverer or a publisher) -----
	cidSource, err := findCidSource(conf)
	if err != nil {
		return nil, errors.Wrap(err, " error generating the CID Tracker")
	}

	studyWG.Add(1)
	if conf.AlreadyPublishedCIDs {
		cidTracker, err := NewCidTracker(ctx, &studyWG, pubordishost, dbInstance, cidSource, cidPinger, conf.K, conf.Workers, reqInterval, studyDuration)
		if err != nil {
			return nil, errors.Wrap(err, "error generating the CidTracker")
		}
		cidDiscoverer, err := NewCidDiscoverer(cidTracker)
		// if the type is http source you need to assign the type to the tracker
		if cidSource.Type() == config.HttpServerSource {
			httpCidSource, ok := cidSource.(*src.HttpCidSource)
			if !ok {
				return nil, errors.New("Couldn't deduce type")
			}
			go httpCidSource.StartServer()
			if err != nil {
				return nil, errors.New("Couldn't start server")
			}
			//assign to user later for graceful shutdown
			cidTracker.httpSource = httpCidSource
		}
		log.Debug("CidHoarder Initialized with cid discoverer")

		return &CidHoarder{
			ctx:        ctx,
			wg:         &studyWG,
			Host:       pubordishost,
			PingerHost: pingerHost,
			DBCli:      dbInstance,
			CidTracker: cidDiscoverer,
			CidPinger:  cidPinger,
		}, nil
	} else {
		cidTracker, err := NewCidTracker(ctx, &studyWG, pubordishost, dbInstance, cidSource, cidPinger, conf.K, conf.Workers, reqInterval, studyDuration)
		if err != nil {
			return nil, errors.Wrap(err, "error generating the CidTracker")
		}
		cidPublisher, err := NewCidPublisher(cidTracker, pubordishost, conf.CidNumber)
		log.Debug("CidHoarder Initialized with cid publisher")
		return &CidHoarder{
			ctx:        ctx,
			wg:         &studyWG,
			Host:       pubordishost,
			PingerHost: pingerHost,
			DBCli:      dbInstance,
			CidTracker: cidPublisher,
			CidPinger:  cidPinger,
		}, nil
	}
}

func (c *CidHoarder) Run() error {
	// Boostrap the kdht host
	err := c.Host.Boostrap(c.ctx)
	if err != nil {
		return errors.Wrap(err, "unable to boostrap the publisher or discoverer host with the kdht routing table.")
	}

	err = c.PingerHost.Boostrap(c.ctx)
	if err != nil {
		return errors.Wrap(err, "unable to boostrap the pinger host with the kdht routing table.")
	}

	// Launch the Cid Tracker
	go c.CidTracker.run()
	go c.CidPinger.Run()

	c.wg.Wait()

	log.Info("study finished! closing everything")

	// after the CidTracker has already finished, close the DB
	c.DBCli.Close()

	return nil
}

// Generates the cid source given a specific config struct. E.g. a randomly generated cid source uses the Random_Cid_Gen struct, this function must return that.
// The options for the cid source are defined in this enum like const value.
//
//	 const (
//		RandomSource   = "random-content-gen"
//		TextFileSource = "text-file"
//		JsonFileSource = "json-file"
//		BitswapSource  = "bitswap"
//	 HttpSource     = "HttpServerSource"
//		RandomContent  = "random"
//	 )
//
//		 const (
//			RandomSource   = "random-content-gen"
//			TextFileSource = "text-file"
//			JsonFileSource = "json-file"
//			BitswapSource  = "bitswap"
//			RandomContent  = "random"
//		 )
func findCidSource(conf *config.Config) (src.CidSource, error) {
	switch conf.CidSource {
	case config.TextFileSource:
		return nil, errors.New("text file source not yet implemented")
	case config.HttpServerSource:
		server := src.NewHttpCidSource(8080, "localhost")
		return server, nil
	case config.JsonFileSource:
		return src.OpenEncodedJSONFile(conf.CidFile)
	case config.RandomSource:
		return src.NewRandomCidGen(conf.CidContentSize, conf.CidNumber), nil
	case config.BitswapSource:
		return nil, errors.New("bitswap source not yet implemented")

	default:
		return nil, errors.New("Could not figure out cid source")
	}
}
