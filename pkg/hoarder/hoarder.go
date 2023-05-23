package hoarder

import (
	"context"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/cortze/ipfs-cid-hoarder/pkg/crawler"
)

type CidHoarder struct {
	ctx context.Context
	wg  *sync.WaitGroup

	dbCli         *db.DBClient

	cidSet *cidSet
	cidPublisher  *CidPublisher
	cidPinger     *CidPinger

	FinishedC chan struct{}
}

func NewCidHoarder(ctx context.Context, conf *config.Config) (*CidHoarder, error) {
	var err error

	// ----- Compose the DB client -----
	dbInstance, err := db.NewDBClient(ctx, conf.Database)
	if err != nil {
		return nil, errors.Wrap(err, "initialise the DB")
	}

	// ------ Configure the settings for the Libp2p hosts ------
	hostOpts := p2p.HostOptions {
		IP: "0.0.0.0",
		Port: conf.Port,
		K: conf.K,
		BlacklistingUA: conf.BlacklistedUA,
	}
	if conf.BlacklistedUA != "" {
		log.Infof("UA blacklisting activated -> crawling network to identify %s (might take 5-7mins)\n", 
			hostOpts.BlacklistingUA,
		)
		// launch light crawler identifying balcklistable peers
		crawlResutls, err := crawler.RunLightCrawler(ctx, conf.BlacklistedUA)
		if err != nil {
			return nil, err
		}
		hostOpts.BlacklistedPeers = crawlResutls.GetBlacklistedPeers()
	}

	//  ------ Study Parameters ---------
	var studyWG sync.WaitGroup
	reqInterval, err := time.ParseDuration(conf.ReqInterval)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing ReqInterval "+conf.ReqInterval)
	}

	cidPingTime, err := time.ParseDuration(conf.CidPingTime)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing StudyDuration "+conf.CidPingTime)
	}
	log.Info("configured Hoarder to request at an interval of ", reqInterval, " and during ", cidPingTime)

	// ----- Generate the CidPinger -----
	cidSet := newCidSet()
	studyWG.Add(1)
	cidPinger, err := NewCidPinger(
		ctx, 
		&studyWG, 
		hostOpts, 
		dbInstance, 
		reqInterval, 
		conf.Workers, 
		cidSet,
	)
	if err != nil {
		return nil, err
	}

	// ---- Generate the CidPublisher -----
	// select the provide operation that we want to perform:
	provOp := StandardDHTProvide
	pubWorkers := 1
	if !conf.SinglePublisher { 
		pubWorkers = conf.Workers 
	}
	studyWG.Add(1)
	cidPublisher, err := NewCidPublisher(
		ctx,
		&studyWG,
		hostOpts,
		provOp,
		dbInstance,
		NewCidGenerator(
			ctx,
			&studyWG,
			conf.CidContentSize,
			conf.CidNumber,
		),
		cidSet,
		conf.K,
		pubWorkers,
		reqInterval,
		cidPingTime,
	)
	if err != nil {
		return nil, err
	}
	return &CidHoarder{
		ctx:           ctx,
		wg:            &studyWG,
		dbCli:         dbInstance,
		cidPublisher:  cidPublisher,
		cidPinger:     cidPinger,
		FinishedC: make(chan struct{}, 1),
	}, nil
}

func (c *CidHoarder) Run() error {
	// Launch the publisher and the pinger
	go c.cidPublisher.Run()
	go c.cidPinger.Run()
	// wait until the app has been closed / finished to notify 
	go func (){
		c.wg.Wait()
		c.dbCli.Close()
		log.Info("hoarder run finished, organically closed")
		c.FinishedC <- struct{}{}
	}()
	return nil
}

func (c *CidHoarder) Close() {
	c.cidPublisher.Close()
	c.cidPinger.Close()
	c.wg.Wait()
	// after the CidTracker has already finished, close the DB
	c.dbCli.Close()
	log.Info("hoarder interruption successfully closed! C ya")
}
