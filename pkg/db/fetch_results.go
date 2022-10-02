package db

import (
	"ipfs-cid-hoarder/pkg/models"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreateFetchResultsTable() error {

	log.Debugf("creating table 'fetch_results' for DB")

	_, err := db.psqlPool.Exec(db.ctx, `
	CREATE TABLE IF NOT EXISTS fetch_results(
		id SERIAL PRIMARY KEY, 
		cid_hash TEXT NOT NULL,
		ping_round INT NOT NULL,
		fetch_time FLOAT NOT NULL,
		fetch_duration FLOAT NOT NULL,
		total_hops INT NOT NULL,
		hops_for_closest INT NOT NULL,
		holders_ping_duration FLOAT NOT NULL,
		find_prov_duration FLOAT NOT NULL,
		get_closest_peer_duration FLOAT NOT NULL,
		k INT NOT NULL,
		success_att INT NOT NULL,
		fail_att INT NOT NULL,
		is_retrievable BOOL NOT NULL,

		UNIQUE(cid_hash, ping_round),
		FOREIGN KEY(cid_hash) REFERENCES cid_info(cid_hash)
	);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for fetch_results table generation")
	}
	return nil
}

func (db *DBClient) addFetchResults(fetchRes *models.CidFetchResults) (err error) {

	log.WithFields(log.Fields{
		"cid": fetchRes.Cid.Hash().B58String(),
	}).Trace("adding fetch_results to DB")

	tot, suc, fail := fetchRes.GetSummary()
	var isRetrievable bool
	if suc > 0 {
		isRetrievable = true
	}

	_, err = db.psqlPool.Exec(db.ctx, `
	INSERT INTO fetch_results (
		cid_hash,
		ping_round,
		fetch_time,
		fetch_duration,
		total_hops,
		hops_for_closest,
		holders_ping_duration,
		find_prov_duration,
		get_closest_peer_duration,
		k,
		success_att,
		fail_att,
		is_retrievable)		 
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		fetchRes.Cid.Hash().B58String(),
		fetchRes.Round,
		fetchRes.StartTime.Unix(),
		fetchRes.FinishTime.Sub(fetchRes.StartTime).Milliseconds(),
		fetchRes.TotalHops,
		fetchRes.HopsToClosest,
		fetchRes.PRHoldPingDuration.Milliseconds(),
		fetchRes.FindProvDuration.Milliseconds(),
		fetchRes.GetClosePeersDuration.Milliseconds(),
		tot,
		suc,
		fail,
		isRetrievable,
	)
	if err != nil {
		return errors.Wrap(err, "unable to insert fetch_results at DB ")
	}

	err = db.addPingResultsSet(fetchRes.PRPingResults)
	if err != nil {
		return errors.Wrap(err, "persisting PingResults")
	}

	err = db.addClosestPeerSet(&models.ClosestPeers{
		Cid:       fetchRes.Cid,
		PingRound: fetchRes.Round,
		Peers:     fetchRes.ClosestPeers,
	})

	return err
}
