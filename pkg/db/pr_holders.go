package db

import (
	"ipfs-cid-hoarder/pkg/models"

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

func (db *DBClient) addPRHoldersSet(c cid.Cid, prHolders []*models.PeerInfo) (err error) {
	if len(prHolders) <= 0 {
		return errors.New("unable to insert pr_holders - no pr_holders set given")
	}
	log.WithFields(log.Fields{
		"cid": c.Hash().B58String(),
	}).Debug("adding set of cid pr_holders to DB")

	// insert each of the Peers holding the PR
	for _, p := range prHolders {
		_, err = db.psqlPool.Exec(db.ctx, `
		INSERT INTO pr_holders (
			cid_hash,
			peer_id)		 
		VALUES ($1, $2)`,
			c.Hash().B58String(),
			p.ID.String(),
		)
		if err != nil {
			return errors.Wrap(err, "unable to insert pr_holders at DB ")
		}

		log.WithFields(log.Fields{
			"cid":        c.Hash().B58String(),
			"pr_holders": len(prHolders),
		}).Trace("tx successfully saved pr_holders into DB")
	}
	return err
}
