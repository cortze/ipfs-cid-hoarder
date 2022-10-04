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

	PubHost    *p2p.Host
	PingerHost *p2p.Host
	DBCli      *db.DBClient
	CidTracker *CidTracker
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
			log.Errorf("unable to parse PrivKey %s", conf.PrivKey)
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
	// generate new fresh key for pinger host
	priv2, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		return nil, errors.Wrap(err, "unable to generate priv key for client's host")
	}

	// ----- Compose Publisher Libp2p host -----
	pubHost, err := p2p.NewHost(ctx, priv, config.CliIp, config.CliPort, conf.K, conf.HydraFilter)
	if err != nil {
		return nil, errors.Wrap(err, "error generating libp2p host for the tool")
	}

	// ----- Compose Pinger Libp2p Host -----

	pingerHost, err := p2p.NewHost(ctx, priv2, config.CliIp, config.CliPort, conf.K, conf.HydraFilter)
	if err != nil {
		return nil, errors.Wrap(err, "error generating libp2p host for the tool")
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
	cidPinger := NewCidPinger(ctx, &studyWG, pingerHost, db, reqInterval, rounds, conf.Workers)

	// ----- Generate the CidTracker -----
	cidSource := NewRandomCidGen(conf.CidContentSize)
	studyWG.Add(1)
	cidTracker, err := NewCidTracker(ctx, &studyWG, pubHost, db, cidSource, cidPinger, conf.K, conf.CidNumber, conf.Workers, reqInterval, studyDuration)
	if err != nil {
		return nil, errors.Wrap(err, "error generating the CidTracker")
	}

	log.Debug("CidHoarder Initialized")

	return &CidHoarder{
		ctx:        ctx,
		wg:         &studyWG,
		PubHost:    pubHost,
		PingerHost: pingerHost,
		DBCli:      db,
		CidTracker: cidTracker,
		CidPinger:  cidPinger,
	}, nil
}

func (c *CidHoarder) Run() error {
	// Boostrap the kdht host
	err := c.PubHost.Boostrap(c.ctx)
	if err != nil {
		return errors.Wrap(err, "unable to boostrap the publisher host with the kdht routing table.")
	}

	err = c.PingerHost.Boostrap(c.ctx)
	if err != nil {
		return errors.Wrap(err, "unable to boostrap the pinger host with the kdht routing table.")
	}

	// Launch the Cid Tracker
	go c.CidTracker.Run()
	go c.CidPinger.Run()

	c.wg.Wait()

	log.Info("study finished! closing everything")

	// after the CidTracker has already finished, close the DB
	c.DBCli.Close()

	return nil
}
