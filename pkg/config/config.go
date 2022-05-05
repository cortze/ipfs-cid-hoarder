package config

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var log = logrus.WithField(
	"module", "config",
)

var DefaultConfig = Config{
	LogLevel: "info",
	Database: "postgres://cid_hoarder:password@127.0.0.1:5432/cid_hoarder",
	GetMode:  "file",
	CidFile:  "cids/cid-list.txt",
}

type Config struct {
	LogLevel string `json:"log-level"`
	Database string `json:"database-enpoint"`
	GetMode  string `json:"get-mode"`
	CidFile  string `json:"cid-file"`
}

// Init takes the command line argumenst from the urfave/cli context and composes the configuration
func Init(ctx *cli.Context) (*Config, error) {
	var c *Config
	c.apply(ctx)

	// TODO: work on reading config file from custom path (reproducibility)
	return c, nil
}

// apply parses the arguments readed from cli.Context
func (c *Config) apply(ctx *cli.Context) {
	// Check if the flags have been set
	if ctx.Command.Name == "get" {
		if ctx.IsSet("log-level") {
			c.LogLevel = ctx.String("log-level")
		}
		if ctx.IsSet("database-enpoint") {
			c.Database = ctx.String("database-enpoint")
		}
		if ctx.IsSet("get-mode") {
			c.GetMode = ctx.String("get-mode")
			// if the TEXT mode was selected, read the file from the cid-file
			if c.GetMode == "text" {
				if ctx.IsSet("cid-file") {
					c.CidFile = ctx.String("cid-file")
				}
			} else if c.GetMode == "bitswap" {
				log.Info("bitswap content discovery not supported yet.")
				os.Exit(0)
			}
		}
	}
}
