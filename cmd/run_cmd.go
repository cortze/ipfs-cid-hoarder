package cmd

import (
	"net/http"
	_ "net/http/pprof"
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
			Name:        "workers",
			Usage:       "max number of CIDs publish and ping workers",
			EnvVars:     []string{"IPFS_CID_HOARDER_BATCH_SIZE"},
			DefaultText: "1 worker",
		},
		&cli.BoolFlag{
			Name:        "single-publisher",
			Usage:       "defines whether the hoarderder uses only a single publisher worker (improves publish time accuracy)",
			EnvVars:     []string{"IPFS_CID_HOARDER_SINGLE_PUBLISHER"},
			DefaultText: "false",
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

	// expose the pprof and prometheus metrics
	go func() {
		pprofAddres := config.PprofIp + ":" + config.PprofPort
		log.Debugf("initializing pprof in %s\n", pprofAddres)
		err := http.ListenAndServe(pprofAddres, nil)
		if err != nil {
			log.Errorf("unable to initialize pprof at %s - error %s", pprofAddres, err.Error())
		}
	}()

	// Initialize the CidHoarder
	log.WithFields(log.Fields{
		"log-level":        conf.LogLevel,
		"port":             conf.Port,
		"database":         conf.Database,
		"cid-size":         conf.CidContentSize,
		"cid-number":       conf.CidNumber,
		"workers":          conf.Workers,
		"single-publisher": conf.SinglePublisher,
		"req-interval":     conf.ReqInterval,
		"cid-ping-time":    conf.CidPingTime,
		"k":                conf.K,
		"prov-op":          conf.ProvideOperation,
		"blacklisted-ua":   conf.BlacklistedUA,
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
			log.Info("cntr-C detected, closing hoarder peacefully")
			cidHoarder.Close()
			break hoarderLoop

		case <-ctx.Context.Done():
			log.Info("context died, closing hoarder peacefully")
			cidHoarder.Close()
			break hoarderLoop

		case <-cidHoarder.FinishedC:
			log.Info("cid hoarder sucessfully finished")
			break hoarderLoop
		}
	}
	return nil
}
