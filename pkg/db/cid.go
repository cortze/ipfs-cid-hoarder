package db

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreateCidInfoTable() error {

	log.Debugf("creating table 'cid_info' for DB")

	_, err := db.psqlPool.Exec(db.ctx, `
		CREATE TABLE IF NOT EXISTS cid_info(
		id SERIAL PRIMARY KEY, 
		cid_hash TEXT NOT NULL,
		gen_time FLOAT NOT NULL,
		provide_time FLOAT NOT NULL,
		req_interval INT NOT NULL,
		k INT NOT NULL,
		content_type TEXT NOT NULL,
		source TEXT NOT NULL,
		creator TEXT NOT NULL,
		
		UNIQUE (cid_hash)
	);`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for CidInfo table generation")
	}

	return nil
}

func (db *DBClient) addCidInfo(cidInfo *models.CidInfo) (err error) {

	log.WithFields(log.Fields{
		"cid": cidInfo.CID.Hash().B58String(),
	}).Trace("adding new cid info to DB")

	// insert the cidInfo
	_, err = db.psqlPool.Exec(db.ctx, `INSERT INTO cid_info (
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
		return err
	}
	return nil
}

func (db *DBClient) GetIdOfCid(cidStr string) (id int, err error) {

	row := db.psqlPool.QueryRow(db.ctx, `SELECT id FROM cid_info WHERE cid_hash=$1;`, cidStr)
	err = row.Scan(&id)
	if err != nil {
		return -1, errors.Wrap(err, "got empty row quering id of cid "+cidStr)
	}
	return id, err
}
