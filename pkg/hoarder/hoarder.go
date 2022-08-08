package hoarder

import (
	"context"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	"github.com/libp2p/go-libp2p-core/crypto"
)

type CidHoarder struct {
	ctx context.Context
	wg  *sync.WaitGroup

	Host       *p2p.Host
	DBCli      *db.DBClient
	CidTracker Tracker
	CidPinger  *CidPinger
}

func NewCidHoarder(ctx context.Context, conf *config.Config) (*CidHoarder, error) {
	var priv crypto.PrivKey
	var err error

	// ----- Compose the DB client -----
	db, err := db.NewDBClient(ctx, conf.Database)
	if err != nil {
		return nil, errors.Wrap(err, "initialise the DB")
	}

	// Read or Generate Priv key for the host
	genKey := true
	if conf.PrivKey != "" {
		priv, err = p2p.ParsePrivateKey(conf.PrivKey)
		if err != nil {
			log.Error("unable to parse PrivKey %s", conf.PrivKey)
		} else {
			genKey = false
			log.Debugf("Readed PrivKey %s", p2p.PrivKeyToString(priv))
		}
	}
	if genKey {
		// generate private and public keys for the Libp2p host
		priv, _, err = crypto.GenerateKeyPair(crypto.Secp256k1, 256)
		if err != nil {
			return nil, errors.Wrap(err, "unable to generate priv key for client's host")
		}
		log.Debugf("Generated Priv Key for the host %s", p2p.PrivKeyToString(priv))
	}

	// ----- Compose the Libp2p host -----
	h, err := p2p.NewHost(ctx, priv, config.CliIp, config.CliPort, conf.K, conf.HydraFilter)
	if err != nil {
		return nil, errors.Wrap(err, "error generating libp2p host for the tool")
	}

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
	cidPinger := NewCidPinger(ctx, &studyWG, h, db, reqInterval, rounds, conf.Workers)

	// ----- Generate the CidTracker -----
	cidSource, err := find_cid_source(conf)
	if err != nil {
		return nil, errors.Wrap(err, " error generating the CID Tracker")
	}
	studyWG.Add(1)

	if conf.AlreadyPublishedCIDs {
		cidTracker, err := NewCidTracker(ctx, &studyWG, h, db, cidSource, cidPinger, conf.K, conf.CidNumber, conf.Workers, reqInterval, studyDuration)
		if err != nil {
			return nil, errors.Wrap(err, "error generating the CidTracker")
		}
		cidDiscoverer, err := NewCidDiscoverer(cidTracker)
		log.Debug("CidHoarder Initialized")

		return &CidHoarder{
			ctx:        ctx,
			wg:         &studyWG,
			Host:       h,
			DBCli:      db,
			CidTracker: cidDiscoverer,
			CidPinger:  cidPinger,
		}, nil
	} else {
		cidTracker, err := NewCidTracker(ctx, &studyWG, h, db, cidSource, cidPinger, conf.K, conf.CidNumber, conf.Workers, reqInterval, studyDuration)
		if err != nil {
			return nil, errors.Wrap(err, "error generating the CidTracker")
		}
		cidPublisher, err := NewCidPublisher(cidTracker)
		log.Debug("CidHoarder Initialized")
		return &CidHoarder{
			ctx:        ctx,
			wg:         &studyWG,
			Host:       h,
			DBCli:      db,
			CidTracker: cidPublisher,
			CidPinger:  cidPinger,
		}, nil
	}
}

func (c *CidHoarder) Run() error {
	// Boostrap the kdht host
	err := c.Host.Boostrap(c.ctx)
	if err != nil {
		return errors.Wrap(err, "unable to boostrap the host with the kdht routing table.")
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

//Generates the cid source given a specific config struct. E.g. a randomly generated cid source uses the Random_Cid_Gen struct, this function must return that.
//The options for the cid source are defined in this enum like const value.
//
//  const (
//	RandomSource   = "random-content-gen"
//	TextFileSource = "text-file"
//	BitswapSource  = "bitswap"
//	RandomContent  = "random"
//  )
//
func find_cid_source(conf *config.Config) (CidSource, error) {
	switch conf.CidSource {
	case config.TextFileSource:
		return newFileCIDSource(conf.CidFile)
	case config.RandomContent:
		return NewRandomCidGen(conf.CidContentSize), nil
	case config.BitswapSource:
		return newBitswapCIDSource(), nil

	default:
		return nil, errors.New("Could not figure out cid source")
	}
}
