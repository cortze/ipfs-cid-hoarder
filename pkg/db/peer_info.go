package db

import (
	"context"
	"database/sql"

	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreatePeerInfoTable() error {
	log.Debugf("creating table 'peer_info' for SQLite3 DB")

	// TODO: missing multiaddrs
	stmt := `CREATE TABLE IF NOT EXISTS peer_info(
		id INTEGER PRIMARY KEY AUTOINCREMENT, 
		cid INTEGER NOT NULL,
		peer_id TEXT NOT NULL,
		user_agent TEXT NOT NULL,
		client TEXT NOT NULL,
		version TEXT NOT NULL,

		CONSTRAINT pr_holder UNIQUE (cid, peer_id)
		FOREIGN KEY(cid) REFERENCES cid_info(cid_hash)
	);`

	_, err := db.sqlCli.Exec(stmt)
	if err != nil {
		return errors.Wrap(err, "unable to create table peer_info ")
	}

	return nil
}

func (db *DBClient) AddNewPeerInfoSet(ctx context.Context, c *cid.Cid, pInfos []*models.PeerInfo) (err error) {
	log.WithFields(log.Fields{
		"cid": c.Hash().B58String(),
	}).Trace("adding set of peer infos to DB")

	// Should be threadsafe?¿?
	// db.m.Lock()
	// defer db.m.Unlock()

	tx, err := db.sqlCli.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "unable to begin transaction to add new PeerInfoSet ")
	}

	// commit or rollback the tx depending on the error
	defer func() {
		if err != nil {
			tx.Rollback()
			err = errors.Wrap(err, "unable to add new peer_info, rollback the tx ")
		}
		err = tx.Commit()
		log.WithFields(log.Fields{
			"cid":   c.Hash().B58String(),
			"peers": len(pInfos),
		}).Trace("tx successfully saved peer_info into DB")
	}()

	stmt, err := tx.Prepare(`INSERT INTO peer_info (
		cid,
		peer_id,
		user_agent,
		client,
		version) 
	VALUES ($1, $2, $3, $4, $5)`)
	if err != nil {
		return errors.Wrap(err, "unable to prepare insert query for peer_info at SQLite3 DB ")
	}

	id, err := db.GetIdOfCid(c.Hash().B58String())
	if err != nil {
		return errors.Wrap(err, "unable to insert peerInfo ")
	}

	// insert each of the Peers holding the PR
	for _, p := range pInfos {
		_, err = stmt.Exec(
			id,
			p.ID.String(),
			p.UserAgent,
			p.Client,
			p.Version,
		)
		if err != nil {
			return errors.Wrap(err, "unable to insert peer info at SQLite3 DB ")
		}
	}

	return err
}

func (db *DBClient) AddNewPeerInfo(c *cid.Cid, pInfo *models.PeerInfo) (err error) {
	log.WithFields(log.Fields{
		"cid": c.Hash().B58String(),
	}).Trace("adding new peer info to DB")

	// Should be threadsafe?¿?
	// db.m.Lock()
	// defer db.m.Unlock()

	tx, err := db.sqlCli.BeginTx(db.ctx, nil)
	if err != nil {
		return errors.Wrap(err, "unable to begin transaction to add new PeerInfo ")
	}

	// commit or rollback the tx depending on the error
	defer func() {
		if err != nil {
			tx.Rollback()
			err = errors.Wrap(err, "unable to add new cid info, rollback the tx ")
		}
		err = tx.Commit()
		log.WithFields(log.Fields{
			"cid": c.Hash().B58String(),
		}).Trace("tx successfully saved peer_info into DB")
	}()

	// insert each of the Peers holding the PR
	err = db.insertPeerInfo(tx, c, pInfo)
	if err != nil {
		return errors.Wrap(err, "unable to insert peer_info at SQLite3 DB ")
	}

	return err
}

func (db *DBClient) insertPeerInfo(tx *sql.Tx, c *cid.Cid, peerInfo *models.PeerInfo) error {
	id, err := db.GetIdOfCid(c.Hash().B58String())
	if err != nil {
		return errors.Wrap(err, "unable to insert peer_info ")
	}

	// insert the cidInfo
	_, err = tx.Exec(`INSERT INTO peer_info (
			cid,
			peer_id,
			user_agent,
			client,
			version) 
		VALUES ($1, $2, $3, $4, $5)`,
		id,
		peerInfo.ID.String(),
		peerInfo.UserAgent,
		peerInfo.Client,
		peerInfo.Version,
	)
	if err != nil {
		return err
	}
	return nil
}

func (db *DBClient) GetIdOfPeer(pIdStr string) (id int, err error) {

	err = db.sqlCli.QueryRow(`SELECT id FROM peer_info WHERE peer_id=$1`, pIdStr).Scan(&id)
	if err != nil {
		return -1, errors.Wrap(err, "got empty row quering id of cid "+pIdStr)
	}
	return id, err
}
