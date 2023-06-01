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
		cid_hash TEXT NOT NULL,
		ping_round INT NOT NULL,
		fetch_time TIMESTAMP NOT NULL,
		fetch_time_since_publication_m FLOAT NOT NULL,
		fetch_duration_ms FLOAT NOT NULL,
		total_hops INT NOT NULL,
		hops_tree_depth INT NOT NULL,
		min_hops_for_closest INT NOT NULL,
		holders_ping_duration FLOAT NOT NULL,
		find_prov_duration FLOAT NOT NULL,
		get_closest_peer_duration FLOAT NOT NULL,
		k INT NOT NULL,
		success_att INT NOT NULL,
		fail_att INT NOT NULL,
		is_retrievable BOOL NOT NULL,
		pr_with_maddrs BOOL NOT NULL,

		UNIQUE(cid_hash, ping_round),
		FOREIGN KEY(cid_hash) REFERENCES cid_info(cid_hash)
	);

	CREATE INDEX IF NOT EXISTS idx_fetch_results_cid_hash						ON fetch_results (cid_hash);
	CREATE INDEX IF NOT EXISTS idx_fetch_results_ping_round						ON fetch_results (ping_round);
	CREATE INDEX IF NOT EXISTS idx_fetch_results_fetch_time						ON fetch_results (fetch_time);
	CREATE INDEX IF NOT EXISTS idx_fetch_results_fetch_time_since_publication_m	ON fetch_results (fetch_time_since_publication_m);
	`)
	if err != nil {
		return errors.Wrap(err, "error preparing statement for fetch_results table generation")
	}
	return nil
}

func (db *DBClient) addFetchResults(fetchRes *models.CidFetchResults) persistable {
	persis := newPersistable()

	persis.query = `
	INSERT INTO fetch_results (
		cid_hash,
		ping_round,
		fetch_time,
		fetch_time_since_publication_m,
		fetch_duration_ms,
		total_hops,	
		hops_tree_depth,
		min_hops_for_closest,
		holders_ping_duration,
		find_prov_duration,
		get_closest_peer_duration,
		k,
		success_att,
		fail_att,
		is_retrievable,
		pr_with_maddrs)		 
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`

	tot, suc, fail := fetchRes.GetSummary()

	persis.values = append(persis.values,
		fetchRes.Cid.Hash().B58String(),
		fetchRes.Round,
		fetchRes.StartTime,
		fetchRes.GetFetchTimeSincePublication().Minutes(),
		fetchRes.FinishTime.Sub(fetchRes.StartTime).Milliseconds(),
		fetchRes.TotalHops,
		fetchRes.HopsTreeDepth,
		fetchRes.MinHopsToClosest,
		fetchRes.PRHoldPingDuration.Milliseconds(),
		fetchRes.FindProvDuration.Milliseconds(),
		fetchRes.GetClosePeersDuration.Milliseconds(),
		tot,
		suc,
		fail,
		fetchRes.IsRetrievable,
		fetchRes.PRWithMAddr)

	return persis
}
