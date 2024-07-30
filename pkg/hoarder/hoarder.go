package hoarder

import (
	"context"
	"sync"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/db"
	"github.com/cortze/ipfs-cid-hoarder/pkg/metrics"
	"github.com/cortze/ipfs-cid-hoarder/pkg/p2p"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/cortze/ipfs-cid-hoarder/pkg/crawler"
)

type CidHoarder struct {
	ctx context.Context
	wg  *sync.WaitGroup

	dbCli        *db.DBClient
	cidSet       *cidSet
	cidPublisher *CidPublisher
	cidPinger    *CidPinger
	prometheus   *metrics.PrometheusMetrics

	FinishedC chan struct{}
}

func NewCidHoarder(ctx context.Context, conf *config.Config) (*CidHoarder, error) {
	var err error
	var studyWG sync.WaitGroup
	cidSet := newCidSet()

	// ----- Compose the DB client -----
	dbInstance, err := db.NewDBClient(ctx, conf.Database)
	if err != nil {
		return nil, errors.Wrap(err, "initialise the DB")
	}

	// ------ Configure the settings for the Libp2p hosts ------
	hostOpts := p2p.DHTHostOptions{
		IP:             "0.0.0.0",
		Port:           conf.Port,
		ProvOp:         p2p.GetProvOpFromConf(conf.ProvideOperation),
		WithNotifier:   false,
		K:              conf.K,
		BlacklistingUA: conf.BlacklistedUA,
	}
	if conf.BlacklistedUA != "" {
		log.Infof("UA blacklisting activated -> crawling network to identify %s (might take 5-7mins)",
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
	reqInterval, err := time.ParseDuration(conf.ReqInterval)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing ReqInterval "+conf.ReqInterval)
	}

	pubInterval, err := time.ParseDuration(conf.PubInterval)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing PubInterval "+conf.PubInterval)
	}

	taskTimeout, err := time.ParseDuration(conf.TaskTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing TaskTimeout "+conf.TaskTimeout)
	}

	cidPingTime, err := time.ParseDuration(conf.CidPingTime)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing StudyDuration "+conf.CidPingTime)
	}

	// ----- Generate the CidPinger -----
	pingerHostOpts := hostOpts
	cidPinger, err := NewCidPinger(
		ctx,
		&studyWG,
		pingerHostOpts,
		dbInstance,
		reqInterval,
		taskTimeout,
		conf.Pingers,
		conf.Hosts,
		cidSet)
	if err != nil {
		return nil, err
	}

	// ---- Generate the CidPublisher -----
	// select the provide operation that we want to perform:
	publisherHostOpts := hostOpts
	publisherHostOpts.WithNotifier = true // the only time were want to have the notifier
	cidPublisher, err := NewCidPublisher(
		ctx,
		&studyWG,
		publisherHostOpts,
		dbInstance,
		NewCidGenerator(
			ctx,
			conf.CidContentSize,
			conf.CidNumber,
		),
		cidSet,
		conf.K,
		conf.Publishers,
		taskTimeout,
		reqInterval,
		pubInterval,
		cidPingTime,
	)
	if err != nil {
		return nil, err
	}

	prometheusMetrics := metrics.NewPrometheusMetrics(
		ctx,
		conf.MetricsIP,
		conf.MetricsPort)

	cidHoarder := &CidHoarder{
		ctx:          ctx,
		wg:           &studyWG,
		dbCli:        dbInstance,
		cidSet:       cidSet,
		cidPublisher: cidPublisher,
		cidPinger:    cidPinger,
		prometheus:   prometheusMetrics,
		FinishedC:    make(chan struct{}, 1),
	}
	return cidHoarder, nil
}

func (c *CidHoarder) Run() error {
	go c.cidPublisher.Run()
	go c.cidPinger.Run()

	// gather all the service metrics into the prometheus service
	hoarderMetrics := c.GetMetrics()
	c.prometheus.AddMeticsModule(hoarderMetrics)
	c.prometheus.Start()

	hlog := log.WithField("mod", "hoarder")
	go func() {
		<-c.cidPublisher.FinishedC
		<-c.cidPinger.FinishedC
		hlog.Info("publisher and pinger successfully closed")
		c.dbCli.Close()
		c.prometheus.Close()
		hlog.Info("run finished, organically closed")
		c.FinishedC <- struct{}{}
	}()
	return nil
}

func (c *CidHoarder) Close() {
	log.Info("hoarder interruption detected!")
	c.cidPublisher.Close()
	c.cidPinger.Close()
}
