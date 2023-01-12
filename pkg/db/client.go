package db

import (
	"context"
	"os"
	"sync"

	"github.com/cortze/ipfs-cid-hoarder/pkg/models"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const bufferSize = 20000
const maxPersisters = 1

type DBClient struct {
	ctx context.Context
	m   sync.RWMutex

	connectionUrl string // the url might not be necessary (better to remove it?¿)
	psqlPool      *pgxpool.Pool

	// Req Channels
	cidInfoC     chan *models.CidInfo
	peerInfoC    chan *models.PeerInfo
	fetchResultC chan *models.CidFetchResults
	pingResultsC chan []*models.PRPingResults

	persistC chan interface{}

	doneC chan struct{}
}

func RemoveOldDBIfExists(oldDBPath string) {
	os.Remove(oldDBPath)
}

// NewDBClient creates and returns a db.cli to persist data into a PostgreSQL database
func NewDBClient(ctx context.Context, url string) (*DBClient, error) {
	logEntry := log.WithFields(log.Fields{
		"db": url,
	},
	)
	logEntry.Debug("initialising the db")

	psqlPool, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialise Posgres DB at "+url)
	}

	// Ping database to verify connection.
	if err = psqlPool.Ping(ctx); err != nil {
		return nil, errors.Wrap(err, "pinging database")
	}

	dbCli := &DBClient{
		ctx:           ctx,
		connectionUrl: url,
		psqlPool:      psqlPool,
		cidInfoC:      make(chan *models.CidInfo, bufferSize),
		peerInfoC:     make(chan *models.PeerInfo, bufferSize),
		fetchResultC:  make(chan *models.CidFetchResults, bufferSize),
		pingResultsC:  make(chan []*models.PRPingResults, bufferSize),
		persistC:      make(chan interface{}, bufferSize),
		doneC:         make(chan struct{}),
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
// TODO: originally created for cuncurrency issues with SQLite3, might not be needed now with PostgreSQL
func (db *DBClient) runPersisters() {
	log.Info("Initializing DB persisters")

	var persisterWG sync.WaitGroup
	for persister := 1; persister <= maxPersisters; persister++ {
		persisterWG.Add(4)
		go func(wg *sync.WaitGroup, persisterID int) {
			defer wg.Done()
			logEntry := log.WithField("persister", persisterID)
			for {
				// give priority to the dbDone channel if it closed
				select {
				case <-db.doneC:
					logEntry.Info("finish detected, closing persister")
					return
				default:
				}

				select {
				case p := <-db.persistC:
					switch p.(type) {
					case (*models.CidInfo):
						cidInfo := p.(*models.CidInfo)
						logEntry.Debugf("persisting CID %s into DB", cidInfo.CID.Hash().B58String())
						err := db.addCidInfo(cidInfo)
						if err != nil {
							logEntry.Error("error persisting CidInfo - " + err.Error())
						}
					case (*models.PeerInfo):
						dbPeer := p.(*models.PeerInfo)
						logEntry.Debugf("persisting peer %s into DB", dbPeer.ID.String())
						err := db.addPeerInfo(dbPeer)
						if err != nil {
							logEntry.Error("error persisting PeerInfo - " + err.Error())
						}
					case (*models.CidFetchResults):
						fetchRes := p.(*models.CidFetchResults)
						logEntry.Debugf("persisting fetch info into DB")
						err := db.addFetchResults(fetchRes)
						if err != nil {
							logEntry.Error("error persisting FetchResults - " + err.Error())
						}
					}
				case <-db.ctx.Done():
					logEntry.Info("shutdown detected, closing persister")
					return
				}
			}
		}(&persisterWG, persister)

	}

	persisterWG.Wait()
	log.Info("Done persisting study")

	close(db.cidInfoC)
	close(db.peerInfoC)
	close(db.fetchResultC)
	close(db.pingResultsC)
	close(db.persistC)
	close(db.doneC)
}

func (db *DBClient) AddCidInfo(c *models.CidInfo) {
	db.persistC <- c
}

func (db *DBClient) AddPeerInfo(p *models.PeerInfo) {
	db.persistC <- p
}

func (db *DBClient) AddFetchResult(f *models.CidFetchResults) {
	db.persistC <- f
}

// Close closes all the connections opened with the DB
// TODO: still not completed
func (db *DBClient) Close() {
	log.Info("closing DB for the CID Hoarder")
	// send message on doneC
	db.doneC <- struct{}{}
	// db.psqlPool.Close()
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
