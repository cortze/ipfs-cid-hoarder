package db

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreateFetchResultsTable() error {

	log.Debugf("creating table 'fetch_results' for DB")

	_, err := db.psqlPool.Exec(db.ctx, `
	CREATE TABLE IF NOT EXISTS fetch_results(
		id SERIAL PRIMARY KEY, 
		cid INT NOT NULL,
		fetch_round INT NOT NULL,
		fetch_time FLOAT NOT NULL,
		k INT NOT NULL,
		success_att INT NOT NULL,
		fail_att INT NOT NULL,
		is_retrievable BOOL NOT NULL
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

	contId, err := db.GetIdOfCid(fetchRes.Cid.Hash().B58String())
	if err != nil {
		return err
	}

	tot, suc, fail := fetchRes.GetSummary()
	var isRetrievable bool
	if suc > 0 {
		isRetrievable = true
	}

	_, err = db.psqlPool.Exec(db.ctx, `
	INSERT INTO fetch_results (
		cid,
		fetch_round,
		fetch_time,
		k,
		success_att,
		fail_att,
		is_retrievable)		 
	VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		contId,
		fetchRes.Round,
		fetchRes.FinishTime.Sub(fetchRes.StartTime).Milliseconds(),
		tot,
		suc,
		fail,
		isRetrievable,
	)
	if err != nil {
		return errors.Wrap(err, "unable to insert fetch_results at DB ")
	}

	err = db.addClosestPeerSet(fetchRes.Cid, fetchRes.Round, fetchRes.ClosestPeers)

	return err
}
