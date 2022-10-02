package db

import (
	"ipfs-cid-hoarder/pkg/models"

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
	);`)
	if err != nil {
		return errors.Wrap(err, "creating table peer_info ")
	}

	return nil
}

func (db *DBClient) addNewPeerInfoSet(pInfos []*models.PeerInfo) (err error) {
	if len(pInfos) <= 0 {
		return errors.New("unable to insert peer_info set - no peer_info set given")
	}
	log.WithFields(log.Fields{
		"peerIDs": len(pInfos),
	}).Trace("adding set of peer infos to DB")

	// insert each of the Peers holding the PR
	for _, p := range pInfos {
		err = db.addPeerInfo(p)
		if err != nil {
			return errors.Wrap(err, "unable to insert peer info at DB ")
		}
	}

	return err
}

func (db *DBClient) addPeerInfo(pInfo *models.PeerInfo) (err error) {

	log.Tracef("adding new peer %s info to DB", pInfo.ID.String())

	// insert the cidInfo
	_, err = db.psqlPool.Exec(db.ctx, `INSERT INTO peer_info (
			peer_id,
			multi_addrs,
			user_agent,
			client,
			version) 
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`,
		pInfo.ID.String(),
		pInfo.AddrInfo,
		pInfo.UserAgent,
		pInfo.Client,
		pInfo.Version,
	)
	if err != nil {
		return err
	}

	return err
}

func (db *DBClient) GetIdOfPeer(pIdStr string) (id int, err error) {

	err = db.psqlPool.QueryRow(db.ctx, `SELECT id FROM peer_info WHERE peer_id=$1`, pIdStr).Scan(&id)
	if err != nil {
		return -1, errors.Wrap(err, "got empty row quering id of cid "+pIdStr)
	}
	return id, err
}
