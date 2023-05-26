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
			id SERIAL, 
			cid_hash TEXT NOT NULL PRIMARY KEY,
			pub_time TIMESTAMP NOT NULL,
			provide_time_ms FLOAT NOT NULL,
			req_interval_m INT NOT NULL,
			k INT NOT NULL,
			prov_op TEXT NOT NULL,
			creator TEXT NOT NULL
		);
				
		CREATE INDEX IF NOT EXISTS idx_cid_info_cid_hash			ON cid_info (cid_hash);
		CREATE INDEX IF NOT EXISTS idx_cid_info_pub_time			ON cid_info (pub_time);
		CREATE INDEX IF NOT EXISTS idx_cid_info_provide_time_ms		ON cid_info (provide_time_ms);
		CREATE INDEX IF NOT EXISTS idx_cid_info_prov_op				ON cid_info (prov_op);
	`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for CidInfo table generation")
	}

	return nil
}

func (db *DBClient) addCidInfo(cidInfo *models.CidInfo) persistable {
	persis := newPersistable()
	persis.query = `INSERT INTO cid_info(
		cid_hash,
		pub_time,
		provide_time_ms,
		req_interval_m,
		k,
		prov_op,
		creator) 
	VALUES($1, $2, $3, $4, $5, $6, $7)`

	persis.values = append(persis.values, cidInfo.CID.Hash().B58String())
	persis.values = append(persis.values, cidInfo.PublishTime)
	persis.values = append(persis.values, cidInfo.ProvideTime.Milliseconds())
	persis.values = append(persis.values, int(cidInfo.ReqInterval.Minutes()))
	persis.values = append(persis.values, cidInfo.K)
	persis.values = append(persis.values, cidInfo.ProvideOp)
	persis.values = append(persis.values, cidInfo.Creator.String())

	return persis	
}

func (db *DBClient) GetIdOfCid(cidStr string) (id int, err error) {
	row := db.psqlPool.QueryRow(db.ctx, `SELECT id FROM cid_info WHERE cid_hash=$1;`, cidStr)
	err = row.Scan(&id)
	if err != nil {
		return -1, errors.Wrap(err, "got empty row quering id of cid "+cidStr)
	}
	return id, err
}
