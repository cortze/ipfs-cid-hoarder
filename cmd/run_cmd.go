package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/hoarder"

	log "github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

var RunCmd = &cli.Command{
	Name:   "run",
	Usage:  "starts requesting CIDs from the IPFS network from the given source",
	Action: RunHoarder,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "port",
			Usage:       "port number where the hoarder will be spawned",
			EnvVars:     []string{"IPFS_CID_HOARDER_PORT"},
			DefaultText: "9010",
		},
		&cli.StringFlag{
			Name:        "metrics-ip",
			Usage:       "IP where the hoarder will post prometheus and pprof metrics",
			EnvVars:     []string{"IPFS_CID_HOARDER_METRICS_IP"},
			DefaultText: "127.0.0.1",
		},
		&cli.StringFlag{
			Name:        "metrics-port",
			Usage:       "port number where the hoarder will post prometheus and pprof metrics",
			EnvVars:     []string{"IPFS_CID_HOARDER_METRICS_PORT"},
			DefaultText: "9022",
		},
		&cli.StringFlag{
			Name:        "log-level",
			Usage:       "verbosity of the logs that will be displayed [debug,warn,info,error]",
			EnvVars:     []string{"IPFS_CID_HOARDER_LOGLEVEL"},
			DefaultText: "info",
		},
		&cli.StringFlag{
			Name:        "database-endpoint",
			Usage:       "database endpoint",
			EnvVars:     []string{"IPFS_CID_HOARDER_DATABASE_ENDPOINT"},
			DefaultText: "postgresql://user:password@localhost:5432/database",
			Required:    true,
		},
		&cli.IntFlag{
			Name:        "cid-content-size",
			Usage:       "size in KB of the random block generated",
			EnvVars:     []string{"IPFS_CID_HOARDER_CID_CONTENT_SIZE"},
			DefaultText: "1024 (1KB)",
		},
		&cli.IntFlag{
			Name:        "cid-number",
			Usage:       "number of CIDs that will be generated for the study, (set to -1 for a contineous measurement)",
			EnvVars:     []string{"IPFS_CID_HOARDER_CID_NUMBER"},
			DefaultText: "Undefined CIDs",
		},
		&cli.IntFlag{
			Name:        "publishers",
			Usage:       "number of concurrent CID publishers that will be spawned",
			EnvVars:     []string{"IPFS_CID_HOARDER_PUBLISHERS"},
			DefaultText: "default: 1",
		},
		&cli.IntFlag{
			Name:        "pingers",
			Usage:       "number of concurrent pingers that will execute the ping tasks",
			EnvVars:     []string{"IPFS_CID_HOARDER_PINGERS"},
			DefaultText: "default: 250",
		},
		&cli.IntFlag{
			Name:        "hosts",
			Usage:       "number of libp2p hosts that will be used by the ping workers",
			EnvVars:     []string{"IPFS_CID_HOARDER_HOSTS"},
			DefaultText: "default: 1",
		},
		&cli.StringFlag{
			Name:        "pub-interval",
			Usage:       "delay between the publication of each CID (example '180s' - '60s')",
			EnvVars:     []string{"IPFS_CID_HOARDER_PUB_TIME_DELAY"},
			DefaultText: "80s",
		},
		&cli.StringFlag{
			Name:        "task-timeout",
			Usage:       "time allocated to perform each of the ping tasks after which it will consider a failed task (example '80s' - '60s')",
			EnvVars:     []string{"IPFS_CID_HOARDER_TASK_TIMEOUT"},
			DefaultText: "80s",
		},
		&cli.StringFlag{
			Name:        "req-interval",
			Usage:       "delay in minutes in between PRHolders pings for each CID (example '30m' - '1h' - '60s')",
			EnvVars:     []string{"IPFS_CID_HOARDER_REQ_INTERVAL"},
			DefaultText: "30m",
		},
		&cli.StringFlag{
			Name:        "cid-ping-time",
			Usage:       "max time that each CID will be track for (example '24h', '35h', '48h')",
			EnvVars:     []string{"IPFS_CID_HOARDER_CID_PING_TIME"},
			DefaultText: "48h",
		},
		&cli.IntFlag{
			Name:        "k",
			Usage:       "number of peers that we want to forward the Provider Records",
			EnvVars:     []string{"IPFS_CID_HOARDER_K"},
			DefaultText: "K=20",
		},
		&cli.StringFlag{
			Name:        "prov-op",
			Usage:       "select the algorithm to povide CIDs in the DHT",
			EnvVars:     []string{"IPFS_CID_HOARDER_PROV_OP"},
			DefaultText: "standard/optimistic",
		},
		&cli.StringFlag{
			Name:        "blacklisted-ua",
			Usage:       "user agent that wants to be balcklisted from having interactions with",
			EnvVars:     []string{"IPFS_CID_HOARDER_BLACKLISTED_UA"},
			DefaultText: "no-blacklisting",
		},
	},
}

func RunHoarder(ctx *cli.Context) error {
	// here goes all the magic
	// generate config from the urfave/cli context
	conf := &config.DefaultConfig
	conf.Apply(ctx)

	file, err := config.ParseLogOutput("terminal")
	if err != nil {
		return err
	}
	// set the logs configurations
	log.SetFormatter(config.ParseLogFormatter("text"))
	log.SetOutput(file)
	// log.SetOutput(config.ParseLogOutput("terminal"))
	log.SetLevel(config.ParseLogLevel(conf.LogLevel))

	// Initialize the CidHoarder
	log.WithFields(log.Fields{
		"log-level":      conf.LogLevel,
		"port":           conf.Port,
		"metrics-ip":     conf.MetricsIP,
		"metrics-port":   conf.MetricsPort,
		"database":       conf.Database,
		"cid-size":       conf.CidContentSize,
		"cid-number":     conf.CidNumber,
		"publishers":     conf.Publishers,
		"pingers":        conf.Pingers,
		"hosts":          conf.Hosts,
		"req-interval":   conf.ReqInterval,
		"pub-interval":   conf.PubInterval,
		"task-timeout":   conf.TaskTimeout,
		"cid-ping-time":  conf.CidPingTime,
		"k":              conf.K,
		"prov-op":        conf.ProvideOperation,
		"blacklisted-ua": conf.BlacklistedUA,
	}).Info("running cid-hoarder")
	cidHoarder, err := hoarder.NewCidHoarder(ctx.Context, conf)
	if err != nil {
		return err
	}

	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, syscall.SIGKILL, syscall.SIGTERM, os.Interrupt)

	// lauch the Hoarder
	err = cidHoarder.Run()
	if err != nil {
		return err
	}

	// wait until Hoarder finishes, or untill a stop signal comes
hoarderLoop:
	for {
		select {
		case <-signalC:
			log.Info("ctrl-C detected, closing hoarder peacefully")
			cidHoarder.Close()

		case <-ctx.Context.Done():
			log.Info("context died, closing hoarder peacefully")
			cidHoarder.Close()

		// finishedC will always determine when the Hoarder has finished
		case <-cidHoarder.FinishedC:
			log.Info("cid hoarder sucessfully finished")
			break hoarderLoop
		}
	}
	return nil
}
