package config

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var log = logrus.WithField(
	"module", "config",
)

const (
	RandomSource   = "random-content-gen"
	TextFileSource = "text-file"
	BitswapSource  = "bitswap"

	RandomContent = "random"
)

// Harcoded variables for the tool's profiling
var PprofIp = "127.0.0.1"
var PprofPort = "9020"

// Hardcoded variables for the cli host
var CliIp string = "0.0.0.0"
var CliPort string = "9010"
var UserAgent string = "BSC-Cid-Hoarder"

// default configuration
var DefaultConfig = Config{
	LogLevel:       "info",
	Database:       "./ipfs-hoarder-db.db",
	CidSource:      "random-content-gen",
	CidFile:        "cids/cid-list.txt",
	CidContentSize: 1000, // 1MB in KBs
	CidNumber:      10,
	BatchSize:      250,
	ReqInterval:    30, // in minutes
	K:              20, // K-bucket parameter
}

// Config compiles all the set of flags that can be readed from the user while launching the cli
type Config struct {
	PrivKey        string `json:"priv-key"`
	LogLevel       string `json:"log-level"`
	Database       string `json:"database-enpoint"`
	CidSource      string `json:"cid-source"`
	CidFile        string `json:"cid-file"`
	CidContentSize int    `json:"cid-content-size"`
	CidNumber      int    `json:"cid-number"` // in KBs
	BatchSize      int    `json:"batch-size"`
	ReqInterval    int    `json:"req-interval"`
	K              int    `json:"k"`
}

// Init takes the command line argumenst from the urfave/cli context and composes the configuration
func NewConfig(ctx *cli.Context) (*Config, error) {
	c := &Config{}
	c.apply(ctx)

	// TODO: work on reading config file from custom path/file (reproducibility)
	// 		 export the current conf into a file?
	return c, nil
}

// apply parses the arguments readed from cli.Context
func (c *Config) apply(ctx *cli.Context) {
	// Check if the flags have been set
	if ctx.Command.Name == "run" {
		if ctx.IsSet("priv-key") {
			c.PrivKey = ctx.String("priv-key")
		}
		if ctx.IsSet("log-level") {
			c.LogLevel = ctx.String("log-level")
		}
		if ctx.IsSet("database-endpoint") {
			c.Database = ctx.String("database-endpoint")
		}
		// TODO: Add republish of the records interval
		// 		 Add the content/peer ping interval
		if ctx.IsSet("cid-source") {
			c.CidSource = ctx.String("cid-source")
			// if the TEXT mode was selected, read the file from the cid-file
			switch c.CidSource {
			case RandomSource:
				// check the size of the random content to generate
				if ctx.IsSet("cid-content-size") {
					c.CidContentSize = ctx.Int("cid-content-size")
				}
				// check the number of random CIDs that we want to generate
				if ctx.IsSet("cid-number") {
					c.CidNumber = ctx.Int("cid-number")
				}
				// batch of CIDs for the entire study
				if ctx.IsSet("batch-size") {
					c.BatchSize = ctx.Int("batch-size")
				}
				// Time delay between the each of the PRHolder pings
				if ctx.IsSet("req-interval") {
					c.ReqInterval = ctx.Int("req-interval")
				}
				// check the number of random CIDs that we want to generate
				if ctx.IsSet("k") {
					c.K = ctx.Int("k")
				}
			case TextFileSource:
				if ctx.IsSet("cid-file") {
					c.CidFile = ctx.String("cid-file")
				}
			case BitswapSource:
				log.Info("bitswap content discovery not supported yet.")
				os.Exit(0)
			default:
				log.Info("no cid source was given.")
				os.Exit(0)
			}
		}
	}
}
