package cmd

import (
	"net/http"
	_ "net/http/pprof"

	"ipfs-cid-hoarder/pkg/config"
	"ipfs-cid-hoarder/pkg/hoarder"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

var RunCmd = &cli.Command{
	Name:   "run",
	Usage:  "starts requesting CIDs from the IPFS network from the given source",
	Action: RunHoarder,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "log-level",
			Usage:       "verbosity of the logs that will be displayed [debug,warn,info,error]",
			EnvVars:     []string{"IPFS_CID_HOARDER_LOGLEVEL"},
			DefaultText: "info",
			Value:       config.DefaultConfig.LogLevel,
		},
		&cli.StringFlag{
			Name:    "priv-key",
			Usage:   "Private key to initialize the host (to avoid generating node churn in the network)",
			EnvVars: []string{"IPFS_CID_HOARDER_PRIV_KEY"},
		},
		&cli.StringFlag{
			Name:        "database-endpoint",
			Usage:       "database endpoint",
			EnvVars:     []string{"IPFS_CID_HOARDER_DATABASE_ENDPOINT"},
			DefaultText: "postgresql://user:password@localhost:5432/database",
			Value:       config.DefaultConfig.Database,
		},
		&cli.StringFlag{
			Name:        "cid-source",
			Usage:       "defines the mode where we want to run the tool [random-content-gen, text-file, bitswap]",
			DefaultText: "text",
			Value:       config.DefaultConfig.CidSource,
		},
		&cli.StringFlag{
			Name:        "cid-file",
			Usage:       "link to the file containing the files to track (txt/json files)",
			EnvVars:     []string{"IPFS_CID_HOARDER_CID_FILE"},
			DefaultText: "cids/test.txt",
			Value:       config.DefaultConfig.CidFile,
		},
		&cli.IntFlag{
			Name:        "cid-content-size",
			Usage:       "size in KB of the random block generated",
			EnvVars:     []string{"IPFS_CID_HOARDER_CID_CONTENT_SIZE"},
			DefaultText: "1MB",
			Value:       config.DefaultConfig.CidContentSize,
		},
		&cli.IntFlag{
			Name:        "cid-number",
			Usage:       "number of CIDs that will be generated for the study",
			EnvVars:     []string{"IPFS_CID_HOARDER_CID_NUMBER"},
			DefaultText: "1000 CIDs",
			Value:       config.DefaultConfig.CidNumber,
		},
		&cli.IntFlag{
			Name:        "workers",
			Usage:       "max number of CIDs publish and ping workers",
			EnvVars:     []string{"IPFS_CID_HOARDER_BATCH_SIZE"},
			DefaultText: "250 CIDs",
			Value:       config.DefaultConfig.Workers,
		},
		&cli.StringFlag{
			Name:        "req-interval",
			Usage:       "delay in minutes in between PRHolders pings for each CID (example '30m' - '1h' - '60s')",
			EnvVars:     []string{"IPFS_CID_HOARDER_REQ_INTERVAL"},
			DefaultText: "30m",
			Value:       config.DefaultConfig.ReqInterval,
		},
		&cli.StringFlag{
			Name:        "study-duration",
			Usage:       "max time for the study to run (example '24h', '35h', '48h')",
			EnvVars:     []string{"IPFS_CID_HOARDER_STUDY_DURATION"},
			DefaultText: "48h",
			Value:       config.DefaultConfig.StudyDuration,
		},
		&cli.IntFlag{
			Name:        "k",
			Usage:       "number of peers that we want to forward the Provider Records",
			EnvVars:     []string{"IPFS_CID_HOARDER_K"},
			DefaultText: "K=20",
			Value:       config.DefaultConfig.CidNumber,
		},
		&cli.BoolFlag{
			Name:        "already-published-cids",
			Usage:       "if the cids are already published in the network the tool has to only ping them and not publish them",
			DefaultText: "false",
			Value:       config.DefaultConfig.AlreadyPublishedCIDs,
		},
		&cli.BoolFlag{
			Name:        "hydra-filter",
			Usage:       "boolean representation to activate or not the filter to avoid connections to hydras",
			EnvVars:     []string{"IPFS_CID_HOARDER_HYDRA_FILTER"},
			DefaultText: "false",
			Value:       config.DefaultConfig.HydraFilter,
		},
	},
}

func RunHoarder(ctx *cli.Context) error {
	// here goes all the magic

	// generate config from the urfave/cli context
	conf, err := config.NewConfig(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to generate config from arguments")
	}

	// set the logs configurations
	log.SetFormatter(config.ParseLogFormatter("text"))
	log.SetOutput(config.ParseLogOutput("terminal"))
	log.SetLevel(config.ParseLogLevel(conf.LogLevel))

	log.Debug("get configuration:", conf)

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
	log.Info("Running Cid-Hoarder on mode")
	cidHoarder, err := hoarder.NewCidHoarder(ctx.Context, conf)
	if err != nil {
		return err
	}

	return cidHoarder.Run()
}
