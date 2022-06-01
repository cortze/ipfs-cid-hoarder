package hoarder

import (
	"context"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"
	"github.com/libp2p/go-libp2p-core/crypto"
)

type CidHoarder struct {
	ctx context.Context

	Host       *p2p.Host
	DBCli      *db.DBClient
	CidTracker *CidTracker
}

func NewCidHoarder(ctx context.Context, conf *config.Config) (*CidHoarder, error) {
	var priv crypto.PrivKey
	var err error

	// ----- Compose the DB client -----
	db, err := db.NewDBClient(ctx, conf.Database)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialise the DB")
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
	h, err := p2p.NewHost(ctx, priv, config.CliIp, config.CliPort)
	if err != nil {
		return nil, errors.Wrap(err, "error generating libp2p host for the tool")
	}

	// ----- Generate the CidTracker -----
	cidSource := NewRandomCidGen(conf.CidContentSize)
	cidTracker, err := NewCidTracker(ctx, h, db, cidSource, conf.K, conf.CidNumber, conf.BatchSize, conf.ReqInterval, conf.StudyDuration)
	if err != nil {
		return nil, errors.Wrap(err, "error generating the CidTracker")
	}

	log.Debug("CidHoarder Initialized")

	return &CidHoarder{
		ctx:        ctx,
		Host:       h,
		DBCli:      db,
		CidTracker: cidTracker,
	}, nil
}

func (c *CidHoarder) Run() {
	// Boostrap the kdht host
	err := c.Host.Boostrap(c.ctx)
	if err != nil {
		log.Errorf("unable to boostrap the host with the kdht routing table. %s", err.Error())
	}
	// Launch the Cid Tracker
	c.CidTracker.Run()

	// after the CidTracker has already finished, close the DB
	c.DBCli.Close()

}
