package db

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/models"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func (db *DBClient) CreatePeerInfoTable() error {

	log.Debugf("creating table 'peer_info' for DB")

	_, err := db.psqlPool.Exec(db.ctx,
		`CREATE TABLE IF NOT EXISTS peer_info(
		id SERIAL, 
		peer_id TEXT NOT NULL PRIMARY KEY,
		multi_addrs TEXT[] NOT NULL,
		user_agent TEXT NOT NULL,
		client TEXT NOT NULL,
		version TEXT NOT NULL
	);
	
	CREATE INDEX IF NOT EXISTS idx_peer_info_peer_id	ON peer_info (peer_id);
	CREATE INDEX IF NOT EXISTS idx_peer_info_client	ON peer_info (client);
	`)
	if err != nil {
		return errors.Wrap(err, "creating table peer_info ")
	}

	return nil
}

func (db *DBClient) addNewPeerInfoSet(pInfos []*models.PeerInfo) persistable {
	persis := newPersistable()
	if len(pInfos) <= 0 {
		return persis
	}

	// compose the query
	persis.query = multiValueComposer(`
		INSERT INTO peer_info (
			peer_id,
			multi_addrs,
			user_agent,
			client,
			version)`,
		"ON CONFLICT DO NOTHING",
		len(pInfos),
		5)

	// add the values
	for _, pInfo := range pInfos {
		persis.values = append(persis.values,
			pInfo.ID.String(),
			pInfo.MultiAddr,
			pInfo.UserAgent,
			pInfo.Client,
			pInfo.Version)
	}

	return persis
}

func (db *DBClient) addPeerInfo(pInfo *models.PeerInfo) persistable {
	persis := newPersistable()

	// compose the query
	persis.query = `
		INSERT INTO peer_info (
			peer_id,
			multi_addrs,
			user_agent,
			client,
			version) 
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`

	// add the values
	persis.values = append(persis.values, pInfo.ID.String())
	persis.values = append(persis.values, pInfo.MultiAddr)
	persis.values = append(persis.values, pInfo.UserAgent)
	persis.values = append(persis.values, pInfo.Client)
	persis.values = append(persis.values, pInfo.Version)

	return persis
}

func (db *DBClient) GetIdOfPeer(pIdStr string) (id int, err error) {
	err = db.psqlPool.QueryRow(db.ctx, `SELECT id FROM peer_info WHERE peer_id=$1`, pIdStr).Scan(&id)
	if err != nil {
		return -1, errors.Wrap(err, "got empty row quering id of cid "+pIdStr)
	}
	return id, err
}
