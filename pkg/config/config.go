package config

import (
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var log = logrus.WithField(
	"module", "config",
)

// Harcoded variables for the tool's profiling
var MetricsIp = "127.0.0.1"
var MetricsPort = "9022"
var DefaultBlacklistUserAgent = ""
var DefaultDHTProvideOperation = "standard"

// default configuration
var DefaultConfig = Config{
	Port:             "9010",
	MetricsIP:        MetricsIp,
	MetricsPort:      MetricsPort,
	LogLevel:         "info",
	Database:         "postgres://user:password@ip:port/db",
	CidContentSize:   1024, // 1MB in KBs
	CidNumber:        10,
	Publishers:       1,
	Pingers:          250,
	Hosts:            10,
	PubInterval:      "80s",
	TaskTimeout:      "80s",
	ReqInterval:      "30m",
	CidPingTime:      "48h",
	K:                20,
	ProvideOperation: DefaultDHTProvideOperation,
	BlacklistedUA:    DefaultBlacklistUserAgent,
}

// Config compiles all the set of flags that can be read by the user while launching the cli
type Config struct {
	Port             string `json:"port"`
	MetricsIP        string `json:"metrics-ip"`
	MetricsPort      string `json:"metrics-port"`
	LogLevel         string `json:"log-level"`
	Database         string `json:"database-endpoint"`
	CidContentSize   int    `json:"cid-content-size"`
	CidNumber        int    `json:"cid-number"`
	Publishers       int    `json:"publishers"`
	Pingers          int    `json:"pingers"`
	Hosts            int    `json:"hosts"`
	PubInterval      string `json:"pub-interval"`
	TaskTimeout      string `json:"task-timeout"`
	ReqInterval      string `json:"req-interval"`
	CidPingTime      string `json:"cid-ping-time"`
	K                int    `json:"k"`
	ProvideOperation string `json:"prov-op"`
	BlacklistedUA    string `json:"blacklisted-ua"`
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

		if ctx.IsSet("metrics-ip") {
			c.MetricsIP = ctx.String("metrics-ip")
		}

		if ctx.IsSet("metrics-port") {
			c.MetricsPort = ctx.String("metrics-port")
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

		if ctx.IsSet("publishers") {
			c.Publishers = ctx.Int("publishers")
		}

		if ctx.IsSet("pingers") {
			c.Pingers = ctx.Int("pingers")
		}

		if ctx.IsSet("hosts") {
			c.Hosts = ctx.Int("hosts")
		}

		if ctx.IsSet("pub-interval") {
			c.PubInterval = ctx.String("pub-interval")
		}

		if ctx.IsSet("task-timeout") {
			c.TaskTimeout = ctx.String("task-timeout")
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

		if ctx.IsSet("prov-op") {
			c.ProvideOperation = ctx.String("prov-op")
		}

		if ctx.IsSet("blacklisted-ua") {
			c.BlacklistedUA = ctx.String("blacklisted-ua")
		}
	}
}
