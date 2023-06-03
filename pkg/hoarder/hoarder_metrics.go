package hoarder

import (
	"fmt"
	"time"

	"github.com/cortze/ipfs-cid-hoarder/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	hoarderModeName   = "hoarder"
	hoarderModDetails = "general metrics about the hoarder"

	// List of metrics that we are going to export
	ongoingCids = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: hoarderModeName,
		Name:      "ongoing_cids",
		Help:      "Number of cids that are currently published and being tracked",
	})
	secsToNextPing = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: hoarderModeName,
		Name:      "secs_to_next_ping",
		Help:      "Number of seconds left to ping the next cid",
	})
	totalPublishedCids = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: hoarderModeName,
		Name:      "total_publish_cids",
		Help:      "Total number of cids that have been published since the launch of the hoarder",
	},
		[]string{"provide_op"},
	)
	pingingCidsPerHost = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: hoarderModeName,
		Name:      "pinging_cids_per_host",
		Help:      "Number of cids that each of the libp2p host is concurrently tracking",
	},
		[]string{"host_id"},
	)
)

func (h *CidHoarder) GetMetrics() *metrics.MetricsModule {
	// create a new prometheus metrics module
	metricsMod := metrics.NewMetricsModule(
		hoarderModeName,
		hoarderModDetails)

	// add all the smaller metrics
	metricsMod.AddIndvMetric(
		h.ongoingCidMetrics(),
		h.secsToNextPingMetrics(),
		h.totalPublishedCidsMetrics(),
		h.pingingCidsperHostMetrics())
	return metricsMod
}

func (h *CidHoarder) ongoingCidMetrics() *metrics.IndvMetrics {
	initFn := func() error {
		prometheus.MustRegister(ongoingCids)
		return nil
	}
	updateFn := func() (interface{}, error) {
		cids := h.cidSet.Len()
		ongoingCids.Set(float64(cids))
		return cids, nil
	}

	indvMetrics, err := metrics.NewIndvMetrics(
		"ongoing_cids",
		initFn,
		updateFn)
	if err != nil {
		log.WithField("mod", "hoarder-metrics").Error(err)
		return nil
	}
	return indvMetrics
}

func (h *CidHoarder) secsToNextPingMetrics() *metrics.IndvMetrics {
	initFn := func() error {
		prometheus.MustRegister(secsToNextPing)
		return nil
	}
	updateFn := func() (interface{}, error) {
		validTime, valid := h.cidSet.NextValidTimeToPing()
		if !valid {
			log.WithField("mod", "hoarder-metrics").Warnf(
				fmt.Sprintf("no valid time for next ping [time: %s]", validTime))
		}
		validSecs := validTime.Sub(time.Now()).Seconds()
		secsToNextPing.Set(float64(validSecs))
		return validSecs, nil
	}

	indvMetrics, err := metrics.NewIndvMetrics(
		"secs_to_next_ping",
		initFn,
		updateFn)
	if err != nil {
		log.WithField("mod", "hoarder-metrics").Error(err)
		return nil
	}
	return indvMetrics
}

func (h *CidHoarder) totalPublishedCidsMetrics() *metrics.IndvMetrics {
	initFn := func() error {
		prometheus.MustRegister(totalPublishedCids)
		return nil
	}
	updateFn := func() (interface{}, error) {
		totalCids := h.cidPublisher.GetTotalPublishedCids()
		for op, val := range totalCids {
			totalPublishedCids.WithLabelValues(op).Set(float64(val))
		}
		return totalCids, nil
	}

	indvMetrics, err := metrics.NewIndvMetrics(
		"total_publish_cids",
		initFn,
		updateFn)
	if err != nil {
		log.WithField("mod", "hoarder-metrics").Error(err)
		return nil
	}
	return indvMetrics
}

func (h *CidHoarder) pingingCidsperHostMetrics() *metrics.IndvMetrics {
	initFn := func() error {
		prometheus.MustRegister(pingingCidsPerHost)
		return nil
	}
	updateFn := func() (interface{}, error) {
		hostWorkload := h.cidPinger.GetHostWorkload()
		total := 0
		for host, cids := range hostWorkload {
			pingingCidsPerHost.WithLabelValues(fmt.Sprintf("%d", host)).Set(float64(cids))
			total = total + cids
		}
		pingingCidsPerHost.WithLabelValues("total").Set(float64(total))
		hostWorkload[-1] = total
		return hostWorkload, nil
	}

	indvMetrics, err := metrics.NewIndvMetrics(
		"pinging_cids_per_host",
		initFn,
		updateFn)
	if err != nil {
		log.WithField("mod", "hoarder-metrics").Error(err)
		return nil
	}
	return indvMetrics
}
