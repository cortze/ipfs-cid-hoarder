package config

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var log = logrus.WithField(
	"module", "config",
)

// Harcoded variables for the tool's profiling
var PprofIp = "127.0.0.1"
var PprofPort = "9020"

// Hardcoded variables for the cli host
var CliIp string = "0.0.0.0"
var UserAgent string = "BSC-Cid-Hoarder"

// Blacklisting UserAgents
var DefaultBlacklistUserAgent = ""

// default configuration
var DefaultConfig = Config{
	Port:            "9010",
	LogLevel:        "info",
	Database:        "postgres://user:password@ip:port/db",
	CidContentSize:  1024, // 1MB in KBs
	CidNumber:       10,
	Workers:         250,
	SinglePublisher: true,
	ReqInterval:     "30m",
	CidPingTime:     "48h",
	K:               20,
	BlacklistedUA:   DefaultBlacklistUserAgent,
}

// Config compiles all the set of flags that can be read by the user while launching the cli
type Config struct {
	Port            string `json:"port"`
	LogLevel        string `json:"log-level"`
	Database        string `json:"database-endpoint"`
	CidContentSize  int    `json:"cid-content-size"`
	CidNumber       int    `json:"cid-number"`
	Workers         int    `json:"workers"`
	SinglePublisher bool   `json:"single-publisher"`
	ReqInterval     string `json:"req-interval"`
	CidPingTime     string `json:"cid-ping-time"`
	K               int    `json:"k"`
	BlacklistedUA     string   `json:"blacklisted-ua"`
}

// Init takes the command line argumenst from the urfave/cli context and composes the configuration
func NewConfig(ctx *cli.Context) (*Config, error) {
	c := &Config{}
	c.Apply(ctx)
	return c, nil

}

// apply parses the arguments readed from cli.Context
func (c *Config) Apply(ctx *cli.Context) {
	// Check if the flags have been set
	if ctx.Command.Name == "run" {
		if ctx.IsSet("port") {
			c.Port = ctx.String("port")
		}

		if ctx.IsSet("log-level") {
			c.LogLevel = ctx.String("log-level")
		}

		if ctx.IsSet("database-endpoint") {
			c.Database = ctx.String("database-endpoint")
		}

		if ctx.IsSet("cid-content-size") {
			c.CidContentSize = ctx.Int("cid-content-size")
		}

		if ctx.IsSet("cid-number") {
			c.CidNumber = ctx.Int("cid-number")
		}

		if ctx.IsSet("workers") {
			c.Workers = ctx.Int("workers")
		}

		if ctx.IsSet("single-publisher") {
			c.SinglePublisher = ctx.Bool("single-publisher")
		}

		if ctx.IsSet("req-interval") {
			c.ReqInterval = ctx.String("req-interval")
		}

		if ctx.IsSet("cid-ping-time") {
			c.CidPingTime = ctx.String("cid-ping-time")
		}

		if ctx.IsSet("k") {
			c.K = ctx.Int("k")
		}

		if ctx.IsSet("blacklisted-ua") {
			c.BlacklistedUA = ctx.String("blacklisted-ua")
		}
	}
}
