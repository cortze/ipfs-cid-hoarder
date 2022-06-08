package db

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreatePingResultsTable() error {

	log.Debugf("creating table 'ping_results' for DB")

	_, err := db.psqlPool.Exec(db.ctx, `
		CREATE TABLE IF NOT EXISTS ping_results(
			id SERIAL PRIMARY KEY, 
			cid INT NOT NULL,
			peer_id INT NOT NULL,
			ping_round INT NOT NULL,
			fetch_time FLOAT NOT NULL,
			is_active BOOL NOT NULL,
			has_records BOOL NOT NULL,
			conn_error VARCHAR(500) NOT NULL
		);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for ping_results table generation")
	}

	return nil
}

func (db *DBClient) addPingResultsSet(pingRes []*models.PRPingResults) (err error) {
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

	// insert each of the Peers holding the PR
	for _, ping := range pingRes {

		peerId, err := db.GetIdOfPeer(ping.PeerID.String())
		if err != nil {
			return err
		}

		_, err = db.psqlPool.Exec(db.ctx, `
		INSERT INTO ping_results (
			cid,
			peer_id,
			ping_round,
			fetch_time,
			is_active,
			has_records,
			conn_error)		 
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			contId,
			peerId,
			ping.Round,
			ping.FetchTime.Milliseconds(),
			ping.Active,
			ping.HasRecords,
			ping.ConError,
		)
		if err != nil {
			return errors.Wrap(err, "unable to insert ping_results at DB ")
		}

		log.WithFields(log.Fields{
			"cid":   pingRes[0].Cid.Hash().B58String(),
			"round": pingRes[0].Round,
			"pings": len(pingRes),
		}).Trace("tx successfully saved ping_results into DB")
	}
	return err
}
