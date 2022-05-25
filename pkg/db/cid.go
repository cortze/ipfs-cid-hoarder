package db

import (
	"database/sql"

	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreateCidInfoTable() error {
	log.Debugf("creating table 'cid_info' for SQLite3 DB")

	stmt, err := db.sqlCli.Prepare(`CREATE TABLE IF NOT EXISTS cid_info(
		id INTEGER PRIMARY KEY AUTOINCREMENT, 
		cid_hash TEXT NOT NULL,
		gen_time REAL NOT NULL,
		provide_time REAL NOT NULL,
		req_interval INTEGER NOT NULL,
		k INTEGER NOT NULL,
		content_type TEXT NOT NULL,
		source TEXT NOT NULL,
		creator TEXT NOT NULL,
		
		UNIQUE (cid_hash)
	);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for CidInfo table generation")
	}
	stmt.Exec()

	return nil
}

func (db *DBClient) AddNewCidInfo(cidInfo *models.CidInfo) (err error) {
	log.WithFields(log.Fields{
		"cid": cidInfo.CID.Hash().B58String(),
	}).Trace("adding new cid info to DB")

	// Should be threadsafe?¿?
	// db.m.Lock()
	// defer db.m.Unlock()

	tx, err := db.sqlCli.BeginTx(db.ctx, nil)
	if err != nil {
		return errors.Wrap(err, "unable to begin transaction to add new CidInfo ")
	}

	// commit or rollback the tx depending on the error
	defer func() {
		if err != nil {
			tx.Rollback()
			err = errors.Wrap(err, "unable to add new cid info, rollback the tx ")
		}
		err = tx.Commit()
		log.WithFields(log.Fields{
			"cid": cidInfo.CID.Hash().B58String(),
		}).Trace("tx successfully saved cid info into DB")
	}()

	// insert first the CidInfo
	err = db.insertCidInfo(tx, cidInfo)
	if err != nil {
		return errors.Wrap(err, "unable to insert cid info at SQLite3 DB ")
	}
	return err
}

// insertCidInfo adds new
func (db *DBClient) insertCidInfo(tx *sql.Tx, cidInfo *models.CidInfo) error {
	// insert the cidInfo
	_, err := tx.Exec(`INSERT INTO cid_info (
			cid_hash,
			gen_time,
			provide_time,
			req_interval,
			k,
			content_type,
			source,
			creator) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		cidInfo.CID.Hash().B58String(),
		cidInfo.GenTime.Unix(),
		cidInfo.ProvideTime.Milliseconds(),
		int(cidInfo.ReqInterval.Minutes()),
		cidInfo.K,
		cidInfo.ContentType,
		cidInfo.Source,
		cidInfo.Creator.String(),
	)
	if err != nil {
		log.Error("unable to exec CidInfo insert" + err.Error())
		return err
	}
	return nil
}

func (db *DBClient) GetIdOfCid(cidStr string) (id int, err error) {
	row := db.sqlCli.QueryRow(`SELECT id FROM cid_info WHERE cid_hash=$1;`, cidStr)
	err = row.Scan(&id)
	if err != nil {
		return -1, errors.Wrap(err, "got empty row quering id of cid "+cidStr)
	}
	return id, err
}
