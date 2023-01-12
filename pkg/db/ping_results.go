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
			cid_hash TEXT NOT NULL,
			ping_round INT NOT NULL,
			peer_id TEXT NOT NULL,
			ping_time FLOAT NOT NULL,
			ping_duration FLOAT NOT NULL,
			is_active BOOL NOT NULL,
			has_records BOOL NOT NULL,
			conn_error TEXT NOT NULL,

			UNIQUE(cid_hash, ping_round, peer_id),
			FOREIGN KEY(cid_hash) REFERENCES cid_info(cid_hash),
			FOREIGN KEY(peer_id) REFERENCES peer_info(peer_id)
		);`)
	if err != nil {
		return errors.Wrap(err, "ping_results table")
	}

	return nil
}

func (db *DBClient) addPingResultsSet(pingRes []*models.PRPingResults) (err error) {
	if len(pingRes) <= 0 {
		return errors.New("insert ping result set - no ping_results set given")
	}
	cStr := pingRes[0].Cid.Hash().B58String()
	pingRound := pingRes[0].Round

	log.WithFields(log.Fields{
		"cid": cStr,
	}).Debug("adding set of cid ping_results to DB")

	// insert each of the Peers holding the PR
	for _, ping := range pingRes {

		_, err = db.psqlPool.Exec(db.ctx, `
		INSERT INTO ping_results (
			cid_hash,
			ping_round,
			peer_id,
			ping_time,
			ping_duration,
			is_active,
			has_records,
			conn_error)		 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			cStr,
			ping.Round,
			ping.PeerID.String(),
			ping.FetchTime.Unix(),
			ping.FetchDuration.Milliseconds(),
			ping.Active,
			ping.HasRecords,
			ping.ConError,
		)
		if err != nil {
			return errors.Wrap(err, "insert ping_results ")
		}

		log.WithFields(log.Fields{
			"cid":   cStr,
			"round": pingRound,
			"pings": len(pingRes),
		}).Trace("tx successfully saved ping_results into DB")
	}
	return err
}
