package db

import (
	"context"

	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreatePeerInfoTable() error {

	log.Debugf("creating table 'peer_info' for DB")

	// TODO: missing multiaddrs
	_, err := db.psqlPool.Exec(db.ctx,
		`CREATE TABLE IF NOT EXISTS peer_info(
		id SERIAL PRIMARY KEY, 
		cid INT NOT NULL,
		peer_id VARCHAR(256) NOT NULL,
		multi_addrs VARCHAR(256)[] NOT NULL,
		user_agent VARCHAR(256) NOT NULL,
		client VARCHAR(256) NOT NULL,
		version VARCHAR(256) NOT NULL
	);`)
	if err != nil {
		return errors.Wrap(err, "creating table peer_info ")
	}

	return nil
}

func (db *DBClient) addNewPeerInfoSet(ctx context.Context, c *cid.Cid, pInfos []*models.PeerInfo) (err error) {

	log.WithFields(log.Fields{
		"cid": c.Hash().B58String(),
	}).Trace("adding set of peer infos to DB")

	id, err := db.GetIdOfCid(c.Hash().B58String())
	if err != nil {
		return errors.Wrap(err, "unable to insert peerInfo ")
	}

	// insert each of the Peers holding the PR
	for _, p := range pInfos {
		_, err = db.psqlPool.Exec(db.ctx, `
		INSERT INTO peer_info (
			cid,
			peer_id,
			multi_addrs,
			user_agent,
			client,
			version) 
		VALUES ($1, $2, $3, $4, $5, $6)`,
			id,
			p.ID.String(),
			p.MultiAddr,
			p.UserAgent,
			p.Client,
			p.Version,
		)
		if err != nil {
			return errors.Wrap(err, "unable to insert peer info at DB ")
		}
	}

	return err
}

func (db *DBClient) addPeerInfo(c *cid.Cid, pInfo *models.PeerInfo) (err error) {

	log.WithFields(log.Fields{
		"cid": c.Hash().B58String(),
	}).Trace("adding new peer info to DB")

	id, err := db.GetIdOfCid(c.Hash().B58String())
	if err != nil {
		return errors.Wrap(err, "unable to insert peer_info ")
	}

	// insert the cidInfo
	_, err = db.psqlPool.Exec(db.ctx, `INSERT INTO peer_info (
			cid,
			peer_id,
			multi_addrs,
			user_agent,
			client,
			version) 
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id,
		pInfo.ID.String(),
		pInfo.MultiAddr,
		pInfo.UserAgent,
		pInfo.Client,
		pInfo.Version,
	)
	if err != nil {
		return err
	}

	return err
}

func (db *DBClient) GetIdOfPeer(pIdStr string) (id int, err error) {

	err = db.psqlPool.QueryRow(db.ctx, `SELECT id FROM peer_info WHERE peer_id=$1`, pIdStr).Scan(&id)
	if err != nil {
		return -1, errors.Wrap(err, "got empty row quering id of cid "+pIdStr)
	}
	return id, err
}
