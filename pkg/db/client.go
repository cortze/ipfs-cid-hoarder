package db

import (
	"context"
	"os"
	"sync"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type DBClient struct {
	ctx context.Context
	m   sync.RWMutex // is it necessary?

	dbPath string
	sqlCli *sql.DB
}

func RemoveOldDBIfExists(oldDBPath string) {
	os.Remove(oldDBPath)
}

func NewDBClient(ctx context.Context, dbPath string) (*DBClient, error) {
	logEntry := log.WithFields(log.Fields{
		"Path": dbPath,
	},
	)
	logEntry.Debug("initialising the db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to initialise SQLite3 db at "+dbPath)
	}

	// Ping database to verify connection.
	if err = db.Ping(); err != nil {
		return nil, errors.Wrap(err, "pinging database")
	}

	dbCli := &DBClient{
		ctx:    ctx,
		dbPath: dbPath,
		sqlCli: db,
	}

	err = dbCli.initTables()
	if err != nil {
		return nil, errors.Wrap(err, "unable to initilize the tables for the SQLite3 DB")
	}

	logEntry.Infof("SQLite3 db initialised")
	return dbCli, nil
}

func (db *DBClient) Close() {
	// db.m.Lock()
	// defer db.m.Unlock()

	log.Info("closing SQLite3 DB for the CID Hoarder")
	db.sqlCli.Close()
}

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
	return err
}
