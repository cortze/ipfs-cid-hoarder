package cmd

import (
	"github.com/cortze/ipfs-cid-hoarder/pkg/config"

	"github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

var CmdName = "Get CIDs"
var log = logrus.WithField(
	"cmd", CmdName,
)

var GetCmd = &cli.Command{
	Name:   "get-cids",
	Usage:  "starts requesting CIDs from the IPFS network from the given source",
	Action: GetCids,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "log-level",
			Usage:       "verbosity of the logs that will be displayed [debug,warn,info,error]",
			EnvVars:     []string{"IPFS_CID_HOARDER_LOGLEVEL"},
			DefaultText: "info",
			Value:       config.DefaultConfig.LogLevel,
		},
		&cli.StringFlag{
			Name:        "database-endpoint",
			Usage:       "database enpoint (e.g. postgresql://user:password@localhost:5432/database)",
			EnvVars:     []string{"IPFS_CID_HOARDER_DATABASE_ENDPOINT"},
			DefaultText: "postgres://cid_hoarder:password@127.0.0.1:5432/cid_hoarder",
			Value:       config.DefaultConfig.Database,
		},
		&cli.StringFlag{
			Name:        "get-mode",
			Usage:       "defines the mode where we want to run the tool [text, bitswap]",
			EnvVars:     []string{"IPFS_CID_HOARDER_GETMODE"},
			DefaultText: "text",
			Value:       config.DefaultConfig.GetMode,
		},
		&cli.StringFlag{
			Name:        "cid-file",
			Usage:       "link to the file containing the files to track (txt/json files)",
			EnvVars:     []string{"IPFS_CID_HOARDER_CID_FILE"},
			DefaultText: "cids/test.txt",
			Value:       config.DefaultConfig.CidFile,
		},
	},
}

func GetCids(ctx *cli.Context) error {
	//
	log.Info("getting")
	return nil
}
