package db

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"

	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreatePRHoldersTable() error {
	log.Debugf("creating table 'pr_holders' for DB")
	_, err := db.psqlPool.Exec(db.ctx, `
		CREATE TABLE IF NOT EXISTS pr_holders(
			id SERIAL PRIMARY KEY, 
			cid_hash TEXT NOT NULL,
			peer_id TEXT NOT NULL,

			FOREIGN KEY(cid_hash) REFERENCES cid_info(cid_hash),
			FOREIGN KEY(peer_id) REFERENCES peer_info(peer_id)
		);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for pr_holders table generation")
	}
	return nil
}

func (db *DBClient) addPRHoldersSet(c cid.Cid, prHolders []*models.PeerInfo) persistable {
	persis := newPersistable()
	if len(prHolders) <= 0 {
		return persis
	}
	persis.query = multiValueComposer(`
		INSERT INTO pr_holders (
			cid_hash,
			peer_id)`,
		"", // no appendix to the query
		len(prHolders), // number of Values to insert
		2) // number of items per value (cid_hash, peer_id)

	// add each of the items of the PR holders to the values 	
	for _, p := range prHolders {
		persis.values = append(persis.values, c.Hash().B58String())
		persis.values = append(persis.values, p.ID.String())
	}

	return persis
}
