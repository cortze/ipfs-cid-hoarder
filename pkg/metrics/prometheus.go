package metrics

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	EndpointUrl string = "metrics"

	MetricLoopInterval time.Duration = 15 * time.Second // Default Prometheus Frequency
)

type PrometheusMetrics struct {
	ctx context.Context

	ExposedIp       string
	ExposedPort     string
	EndpointUrl     string
	RefreshInterval time.Duration

	Modules []*MetricsModule

	wg     sync.WaitGroup
	closeC chan struct{}
}

func NewPrometheusMetrics(ctx context.Context, ip, port string) *PrometheusMetrics {
	return &PrometheusMetrics{
		ctx:             ctx,
		ExposedIp:       ip,
		ExposedPort:     port,
		EndpointUrl:     EndpointUrl,
		RefreshInterval: MetricLoopInterval,
		Modules:         make([]*MetricsModule, 0),
		closeC:          make(chan struct{}),
	}
}

func (p *PrometheusMetrics) AddMeticsModule(newMod ...*MetricsModule) {
	p.Modules = append(p.Modules, newMod...)
}

func (p *PrometheusMetrics) Start() error {
	http.Handle("/"+p.EndpointUrl, promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%s", p.ExposedIp, p.ExposedPort), nil))
	}()

	err := p.initPrometheusMetrics()
	if err != nil {
		return errors.Wrap(err, "unable to init prometheus metrics")
	}

	p.wg.Add(1)
	go p.launchMetricsUpdater()

	return nil
}

func (p *PrometheusMetrics) initPrometheusMetrics() error {
	log.Debugf("initializing %d metrics modules", len(p.Modules))
	// iter through all the available modules - and call the
	// mudule.InitMetrics() method
	for _, mod := range p.Modules {
		err := mod.Init()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *PrometheusMetrics) launchMetricsUpdater() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.RefreshInterval)
	for {
		select {
		case <-ticker.C:
			log.Trace("updating values for prometheus metrics")
			// update all the submodules on prometheus
			for _, mod := range p.Modules {
				summary := make(map[string]interface{}, 0)
				modSum := mod.UpdateSummary()
				for key, value := range modSum {
					summary[key] = value
				}
				// compose a message with the give summary
				logFields := log.Fields(modSum)
				log.WithFields(logFields).Infof("summary for %s", mod.Name())
			}

		case <-p.closeC:
			log.Debug("detected a controled shutdown")
			return
		case <-p.ctx.Done():
			log.Debug("detected that context died, shutting down")
			return
		}
	}
}

func (p *PrometheusMetrics) Close() {
	// Init loop for each of the Exporters
	log.Debugf("closing %d prometheus metrics modules", len(p.Modules))
	p.closeC <- struct{}{}
	p.wg.Wait()
	log.Debug("prometheus metrics exporte successfully closed")
}
