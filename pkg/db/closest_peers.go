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
		);

		CREATE INDEX IF NOT EXISTS idx_k_closest_peers_cid_hash		ON k_closest_peers (cid_hash);
		CREATE INDEX IF NOT EXISTS idx_k_closest_peers_ping_round	ON k_closest_peers (ping_round);
		CREATE INDEX IF NOT EXISTS idx_k_closest_peers_peer_id		ON k_closest_peers (peer_id);
		`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for k_closest_peers table generation")
	}

	return nil
}

func (db *DBClient) addClosestPeerSet(closestPeers *models.ClosestPeers) persistable {
	persis := newPersistable()

	if closestPeers.PingRound == 0 || len(closestPeers.Peers) <= 0 {
		return persis
	}

	// compose the multiquery
	persis.query = multiValueComposer(`
		INSERT INTO k_closest_peers (
			cid_hash,
			ping_round,
			peer_id)`,
		"",
		len(closestPeers.Peers), // number of values
		3)                       // number of items on value

	// insert each of the Peers as values
	for _, p := range closestPeers.Peers {
		persis.values = append(persis.values, closestPeers.Cid.Hash().B58String())
		persis.values = append(persis.values, closestPeers.PingRound)
		persis.values = append(persis.values, p.String())
	}

	return persis
}
