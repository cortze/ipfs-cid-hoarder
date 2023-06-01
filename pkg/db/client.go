package db

import (
	"context"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/models"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	batchSize      = 1024
	batchFlushTime = 1 * time.Second
	maxPersisters  = 1
)

type DBClient struct {
	ctx context.Context
	m   sync.RWMutex

	psqlPool *pgxpool.Pool
	persistC chan persistable
	closeCs  []chan struct{}
}

// persistable is the common structure that will be held to the db workers over the
// persistC channel and added to a batch later on
type persistable struct {
	query  string
	values []interface{}
}

func newPersistable() persistable {
	return persistable{
		query:  "",
		values: make([]interface{}, 0),
	}
}

func (persis *persistable) isZero() bool {
	return persis.query == ""
}

// NewDBClient creates and returns a db.cli to persist data into a PostgreSQL database
func NewDBClient(ctx context.Context, url string) (*DBClient, error) {
	logEntry := log.WithFields(log.Fields{"db": url})
	logEntry.Trace("initialising the db")

	psqlPool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialise Posgres DB at "+url)
	}

	// Ping database to verify connection.
	if err = psqlPool.Ping(ctx); err != nil {
		return nil, errors.Wrap(err, "pinging database")
	}

	dbCli := &DBClient{
		ctx:      ctx,
		psqlPool: psqlPool,
		persistC: make(chan persistable, maxPersisters),
		closeCs:  make([]chan struct{}, 0, maxPersisters),
	}

	err = dbCli.initTables()
	if err != nil {
		return nil, errors.Wrap(err, "initialising the tables")
	}

	go dbCli.runPersisters()

	logEntry.Infof("DB initialised")
	return dbCli, nil
}

// runPersisters creates a pool of DB persistors
func (db *DBClient) runPersisters() {
	log.Debug("Initializing DB persisters")

	var persisterWG sync.WaitGroup
	for persisterID := 1; persisterID <= maxPersisters; persisterID++ {
		// create a new closeC channel
		closeC := make(chan struct{}, 1)
		db.closeCs = append(db.closeCs, closeC)
		// launch one persiter more
		persisterWG.Add(1)
		go db.persisterWorker(closeC, &persisterWG, persisterID)
	}

	persisterWG.Wait()
	log.Info("persisters closed")
	close(db.persistC)
	for _, closeC := range db.closeCs {
		close(closeC)
	}
	db.psqlPool.Close()
	log.Info("DB client successfully finished")
}

func (db *DBClient) AddCidInfo(c *models.CidInfo) {
	log.WithFields(log.Fields{
		"event_type": "cid_info",
		"cid":        c.CID.String(),
	}).Trace("new event to perstist")

	db.persistC <- db.addCidInfo(c)
	db.persistC <- db.addNewPeerInfoSet(c.PRHolders)
	db.persistC <- db.addPRHoldersSet(c.CID, c.PRHolders)
}

func (db *DBClient) AddPeerInfo(p *models.PeerInfo) {
	log.WithFields(log.Fields{
		"event_type": "peer_info",
		"peerID":     p.ID.String(),
	}).Trace("new event to perstist")

	db.persistC <- db.addPeerInfo(p)
}

func (db *DBClient) AddFetchResult(f *models.CidFetchResults) {
	log.WithFields(log.Fields{
		"event_type": "fetch_results",
		"cid":        f.Cid.Hash().B58String(),
	}).Trace("new event to perstist")

	db.persistC <- db.addFetchResults(f)
	db.persistC <- db.addPingResultsSet(f.PRPingResults)
	db.persistC <- db.addClosestPeerSet(&models.ClosestPeers{
		Cid:       f.Cid,
		PingRound: f.Round,
		Peers:     f.ClosestPeers,
	})
}

// persisterWorker is the main logic of each of the main DB client persisters
// it batches a range of queries untill the flush time is achieved or the number of queries
// is reached
func (db *DBClient) persisterWorker(closeC chan struct{}, wg *sync.WaitGroup, persisterID int) {
	defer wg.Done()
	logEntry := log.WithField("persister", persisterID)

	// control variables for the persister
	shutdown := false
	batcher := NewQueryBatch(db.ctx, db.psqlPool, batchSize)
	flushTicker := time.NewTicker(batchFlushTime)

	for {
		// check if the routine needs to end
		if shutdown && len(db.persistC) == 0 {
			logEntry.Info("persister finished, C ya!")
			return
		}
		// select over the different events/interruptions that might occur
		select {
		case persis := <-db.persistC:
			if persis.isZero() {
				continue
			}
			batcher.AddQuery(persis)
			if batcher.IsReadyToPersist() {
				logEntry.Trace("batcher full, flishing it")
				batcher.PersistBatch()
			}

		case <-flushTicker.C:
			logEntry.Trace("flush interval reached, persisting batch")
			batcher.PersistBatch()

		case <-closeC:
			logEntry.Info("controled closure detected, closing persister")
			shutdown = true
			continue

		case <-db.ctx.Done():
			logEntry.Info("sudden shutdown detected, closing persister")
			return
		}
	}
}

// initTables creates all the necesary tables in the given DB
func (db *DBClient) initTables() error {
	var err error
	// cid_info table
	err = db.CreateCidInfoTable()
	if err != nil {
		return err
	}
	// peer_info table
	err = db.CreatePeerInfoTable()
	if err != nil {
		return err
	}
	// pr_holders
	err = db.CreatePRHoldersTable()
	if err != nil {
		return err
	}
	// fetch_results table
	err = db.CreateFetchResultsTable()
	if err != nil {
		return err
	}
	// ping_results table
	err = db.CreatePingResultsTable()
	if err != nil {
		return err
	}
	// k_closest_peers
	err = db.CreateClosestPeersTable()
	if err != nil {
		return err
	}
	return err
}

// Close makes sure that all the persisters are notified of the closure
func (db *DBClient) Close() {
	log.Info("orchestrating peacefull DB closing")
	for _, closeC := range db.closeCs {
		closeC <- struct{}{}
	}
}
