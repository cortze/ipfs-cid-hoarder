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
			ping_time TIMESTAMP NOT NULL,
			ping_time_since_publication_m FLOAT NOT NULL,
			ping_duration_ms FLOAT NOT NULL,
			is_active BOOL NOT NULL,
			has_records BOOL NOT NULL,
			records_with_maddrs BOOL NOT NULL,
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

func (db *DBClient) addPingResultsSet(pingRes []*models.PRPingResults) persistable {
	persis := newPersistable()
	if len(pingRes) <= 0 {
		return persis
	}

	persis.query = multiValueComposer(`
		INSERT INTO ping_results (
			cid_hash,
			ping_round,
			peer_id,
			ping_time,
			ping_time_since_publication_m,
			ping_duration_ms,
			is_active,
			has_records,
			records_with_maddrs,
			conn_error)`,
		"",
		len(pingRes), // number of values
		10)           // number of items per value

	// insert each of the Peers holding the PR
	for _, ping := range pingRes {
		persis.values = append(persis.values, ping.Cid.Hash().B58String())
		persis.values = append(persis.values, ping.Round)
		persis.values = append(persis.values, ping.PeerID.String())
		persis.values = append(persis.values, ping.PingTime)
		persis.values = append(persis.values, ping.GetPingTimeSincePublication().Minutes())
		persis.values = append(persis.values, ping.PingDuration.Milliseconds())
		persis.values = append(persis.values, ping.Active)
		persis.values = append(persis.values, ping.HasRecords)
		persis.values = append(persis.values, ping.RecordsWithMAddrs)
		persis.values = append(persis.values, ping.ConError)
	}

	return persis
}
