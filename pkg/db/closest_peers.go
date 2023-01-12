package db

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreateClosestPeersTable() error {

	log.Debugf("creating table 'ping_results' for DB")

	_, err := db.psqlPool.Exec(db.ctx, `
		CREATE TABLE IF NOT EXISTS k_closest_peers(
			id SERIAL PRIMARY KEY, 
			cid_hash TEXT NOT NULL,
			ping_round INT NOT NULL,
			peer_id TEXT NOT NULL,

			FOREIGN KEY(cid_hash) REFERENCES cid_info(cid_hash)
		);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for k_closest_peers table generation")
	}

	return nil
}

func (db *DBClient) addClosestPeerSet(closestPeers *models.ClosestPeers) (err error) {
	// first round has not closest peers
	if closestPeers.PingRound == 0 {
		return nil
	}

	if len(closestPeers.Peers) <= 0 {
		return errors.New("unable to insert closest peers - no closest peers set given")
	}

	cStr := closestPeers.Cid.Hash().B58String()
	log.WithFields(log.Fields{
		"cid": cStr,
	}).Debug("adding set of cid closest_peers to DB")

	// insert each of the Peers holding the PR
	for _, p := range closestPeers.Peers {
		_, err = db.psqlPool.Exec(db.ctx, `
		INSERT INTO k_closest_peers (
			cid_hash,
			ping_round,
			peer_id)		 
		VALUES ($1, $2, $3)`,
			cStr,
			closestPeers.PingRound,
			p.String(),
		)
		if err != nil {
			return errors.Wrap(err, "unable to insert closest_peers at DB ")
		}

		log.WithFields(log.Fields{
			"cid":        cStr,
			"round":      closestPeers.PingRound,
			"closePeers": len(closestPeers.Peers),
		}).Trace("tx successfully saved closest_peers into DB")
	}
	return err
}
