package db

import (
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreateClosestPeersTable() error {

	log.Debugf("creating table 'ping_results' for DB")

	_, err := db.psqlPool.Exec(db.ctx, `
		CREATE TABLE IF NOT EXISTS closest_peers(
			id SERIAL PRIMARY KEY, 
			cid INT NOT NULL,
			ping_round INT NOT NULL,
			peer_id VARCHAR(256) NOT NULL
		);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for ping_results table generation")
	}

	return nil
}

func (db *DBClient) addClosestPeerSet(c cid.Cid, pingRound int, closestPeers []peer.ID) (err error) {
	// first round has not closest peers
	if pingRound == 0 {
		return nil
	}
	// first thing to do is to request the id of the CID
	contId, err := db.GetIdOfCid(c.Hash().B58String())
	if err != nil {
		return err
	}

	if len(closestPeers) <= 0 {
		return errors.New("unable to insert closest peers - no closest peers set given")
	}
	log.WithFields(log.Fields{
		"cid": c.Hash().B58String(),
	}).Debug("adding set of cid closest_peers to DB")

	// insert each of the Peers holding the PR
	for _, p := range closestPeers {
		_, err = db.psqlPool.Exec(db.ctx, `
		INSERT INTO closest_peers (
			cid,
			ping_round,
			peer_id)		 
		VALUES ($1, $2, $3)`,
			contId,
			pingRound,
			p.String(),
		)
		if err != nil {
			return errors.Wrap(err, "unable to insert closest_peers at DB ")
		}

		log.WithFields(log.Fields{
			"cid":        c.Hash().B58String(),
			"round":      pingRound,
			"closePeers": len(closestPeers),
		}).Trace("tx successfully saved closest_peers into DB")
	}
	return err
}
