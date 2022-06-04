package db

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreateFetchResultsTable() error {

	log.Debugf("creating table 'fetch_results' for SQLite3 DB")

	stmt, err := db.sqlCli.Prepare(`CREATE TABLE IF NOT EXISTS fetch_results(
		id INTEGER PRIMARY KEY AUTOINCREMENT, 
		cid INTEGER NOT NULL,
		fetch_round INTEGER NOT NULL,
		fetch_time REAL NOT NULL,
		k INTEGER NOT NULL,
		success_att INTEGER NOT NULL,
		fail_att INTEGER NOT NULL,
		is_retrievable INTEGER NOT NULL,
		
		CONSTRAINT fetch_round UNIQUE (cid, fetch_round)
		FOREIGN KEY(cid) REFERENCES cid_info(cid_hash)
	);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for fetch_results table generation")
	}
	stmt.Exec()

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

	tx, err := db.sqlCli.BeginTx(db.ctx, nil)
	if err != nil {
		return errors.Wrap(err, "unable to begin transaction to add new fetch_results ")
	}

	// commit or rollback the tx depending on the error
	defer func() {
		if err != nil {
			tx.Rollback()
			err = errors.Wrap(err, "unable to add new fetch_results, rollback the tx ")
		} else {
			err = tx.Commit()
			log.WithFields(log.Fields{
				"cid":   fetchRes.Cid.Hash().B58String(),
				"round": fetchRes.Round,
			}).Trace("tx successfully saved fetch infos into DB")
		}
	}()

	stmt, err := tx.Prepare(`INSERT INTO fetch_results (
		cid,
		fetch_round,
		fetch_time,
		k,
		success_att,
		fail_att,
		is_retrievable)		 
	VALUES ($1, $2, $3, $4, $5, $6, $7)`)
	if err != nil {
		return errors.Wrap(err, "unable to prepare insert query for fetch_results at SQLite3 DB ")
	}

	tot, suc, fail := fetchRes.GetSummary()
	var isRetrievable bool
	if suc > 0 {
		isRetrievable = true
	}

	_, err = stmt.Exec(
		contId,
		fetchRes.Round,
		fetchRes.FinishTime.Sub(fetchRes.StartTime).Milliseconds(),
		tot,
		suc,
		fail,
		isRetrievable,
	)
	if err != nil {
		return errors.Wrap(err, "unable to insert fetch_results at SQLite3 DB ")
	}

	return err
}
