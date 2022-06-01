package db

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreatePingResultsTable() error {

	log.Debugf("creating table 'ping_results' for SQLite3 DB")

	stmt, err := db.sqlCli.Prepare(`CREATE TABLE IF NOT EXISTS ping_results(
		id INTEGER PRIMARY KEY AUTOINCREMENT, 
		cid INTEGER NOT NULL,
		peer_id INTEGER NOT NULL,
		ping_round INTEGER NOT NULL,
		fetch_time REAL NOT NULL,
		is_active INTEGER NOT NULL,
		has_records INTEGER NOT NULL,
		conn_error INTEGER NOT NULL,
		
		FOREIGN KEY(cid) REFERENCES cid_info(cid_hash) 
		FOREIGN KEY(peer_id) REFERENCES peer_info(peer_id)
		CONSTRAINT ping UNIQUE(cid, peer_id, ping_round)
	);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for ping_results table generation")
	}
	stmt.Exec()

	return nil
}

func (db *DBClient) AddPingResultsSet(pingRes []*models.PRPingResults) (err error) {
	// first thing to do is to request the id of the CID
	contId, err := db.GetIdOfCid(pingRes[0].Cid.Hash().B58String())
	if err != nil {
		return err
	}

	if len(pingRes) <= 0 {
		return errors.New("unable to insert ping result set - no ping_results set given")
	}
	log.WithFields(log.Fields{
		"cid": pingRes[0].Cid.Hash().B58String(),
	}).Debug("adding set of cid ping_results to DB")

	tx, err := db.sqlCli.BeginTx(db.ctx, nil)
	if err != nil {
		return errors.Wrap(err, "unable to begin transaction to add new PingResSet ")
	}

	// commit or rollback the tx depending on the error
	defer func() {
		if err != nil {
			tx.Rollback()
			err = errors.Wrap(err, "unable to add new set of ping_results, rollback the tx ")
		}
		err = tx.Commit()
		log.WithFields(log.Fields{
			"cid":   pingRes[0].Cid.Hash().B58String(),
			"round": pingRes[0].Round,
			"pings": len(pingRes),
		}).Trace("tx successfully saved ping_results into DB")
	}()

	stmt, err := tx.Prepare(`INSERT INTO ping_results (
		cid,
		peer_id,
		ping_round,
		fetch_time,
		is_active,
		has_records,
		conn_error)		 
	VALUES ($1, $2, $3, $4, $5, $6, $7)`)
	if err != nil {
		return errors.Wrap(err, "unable to prepare insert query for ping_results at SQLite3 DB ")
	}

	// insert each of the Peers holding the PR
	for _, ping := range pingRes {

		peerId, err := db.GetIdOfPeer(ping.PeerID.String())
		if err != nil {
			return err
		}

		_, err = stmt.Exec(
			contId,
			peerId,
			ping.Round,
			ping.FetchTime.Milliseconds(),
			ping.Active,
			ping.HasRecords,
			ping.ConError,
		)
		if err != nil {
			return errors.Wrap(err, "unable to insert ping_results at SQLite3 DB ")
		}
	}
	return err
}
