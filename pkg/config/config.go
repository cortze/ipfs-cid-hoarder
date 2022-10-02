package config

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
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
	RandomContent  = "random"
)

// Harcoded variables for the tool's profiling
var PprofIp = "127.0.0.1"
var PprofPort = "9020"

// Hardcoded variables for the cli host
var CliIp string = "0.0.0.0"
var CliPort string = "9010"
var UserAgent string = "BSC-Cid-Hoarder"

// Blacklisting UserAgents
var DefaultBlacklistUserAgent = "hydra-booster"

// default configuration
var DefaultConfig = Config{
	LogLevel:             "info",
	Database:             "./data/ipfs-hoarder-db.db",
	ConfigJsonFile:       "config.json",
	CidSource:            "random-content-gen",
	AlreadyPublishedCIDs: false,
	CidFile:              "cids/cid-list.json",
	//TODO introduce type of CidFile
	CidContentSize: 1000, // 1MB in KBs
	CidNumber:      10,
	Workers:        250,
	ReqInterval:    "30m",
	StudyDuration:  "48h",
	K:              20, // K-bucket parameter
	HydraFilter:    false,
}

// Config compiles all the set of flags that can be readed from the user while launching the cli
type Config struct {
	PrivKey              string `json:"priv-key"`
	LogLevel             string `json:"log-level"`
	Database             string `json:"database-endpoint"`
	CidSource            string `json:"cid-source"`
	CidFile              string `json:"cid-file"`
	ConfigJsonFile       string `json:"config-json-file"`
	CidContentSize       int    `json:"cid-content-size"`
	CidNumber            int    `json:"cid-number"` // in KBs
	Workers              int    `json:"workers"`
	AlreadyPublishedCIDs bool   `json:"already-published"` //already published CIDs skips the tracking phase of the hoarder.
	ReqInterval          string `json:"req-interval"`
	StudyDuration        string `json:"study-duration"`
	K                    int    `json:"k"`
	HydraFilter          bool   `json:"hydra-filter"`
}

// Init takes the command line argumenst from the urfave/cli context and composes the configuration
func NewConfig(ctx *cli.Context) (*Config, error) {
	c := &Config{}
	// TODO: work on reading config file from custom path/file (reproducibility)
	// 		 export the current conf into a file?
	if ctx.IsSet("name-of-config-json-file") {
		c.ImportConfigFromJsonFile()
	} else {
		c.apply(ctx)
	}

	return c, nil

}

//Exports the config struct into a json file
func (c *Config) ExportConfigIntoJsonFile() error {
	content, err := c.JsonConfig()
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("config.json", content, 0644)
	if err != nil {
		return errors.Wrap(err, " while trying to write struct to file")
	}

	return nil
}

//Imports config struct from json file
func (c *Config) ImportConfigFromJsonFile() error {
	content, err := ioutil.ReadFile("config.json")
	if err != nil {
		return errors.Wrap(err, " while trying to import config from json file")
	}
	err = json.Unmarshal(content, &c)
	if err != nil {
		return errors.Wrap(err, " while trying to unmarshal json config")
	}
	return nil
}

//Returns a json representation of the config struct
func (c *Config) JsonConfig() ([]byte, error) {
	out, err := json.Marshal(c)

	if err != nil {
		return nil, errors.Wrap(err, "while trying to format config struct into json")
	}

	return out, nil
}

// apply parses the arguments readed from cli.Context
func (c *Config) apply(ctx *cli.Context) {
	// Check if the flags have been set
	if ctx.Command.Name == "run" {
		if ctx.IsSet("priv-key") {
			c.PrivKey = ctx.String("priv-key")
		} else {
			c.PrivKey = DefaultConfig.PrivKey
		}
		if ctx.IsSet("log-level") {
			c.LogLevel = ctx.String("log-level")
		} else {
			c.LogLevel = DefaultConfig.LogLevel
		}
		//field database endpoint is required to be set by the user
		if ctx.IsSet("database-endpoint") {
			c.Database = ctx.String("database-endpoint")
		}
		if ctx.IsSet("hydra-filter") {
			c.HydraFilter = ctx.Bool("hydra-filter")
		} else {
			c.HydraFilter = DefaultConfig.HydraFilter
		}
		// Time delay between the each of the PRHolder pings
		if ctx.IsSet("req-interval") {
			c.ReqInterval = ctx.String("req-interval")
		} else {
			c.ReqInterval = DefaultConfig.ReqInterval
		}
		if ctx.IsSet("already-published") {
			c.AlreadyPublishedCIDs = ctx.Bool("already-published")
		} else {
			c.AlreadyPublishedCIDs = DefaultConfig.AlreadyPublishedCIDs
		}
		// Set the study duration time
		if ctx.IsSet("study-duration") {
			c.StudyDuration = ctx.String("study-duration")
		} else {
			c.StudyDuration = DefaultConfig.StudyDuration
		}
		// check the number of random CIDs that we want to generate
		if ctx.IsSet("k") {
			c.K = ctx.Int("k")
		} else {
			c.K = DefaultConfig.K
		}
		if ctx.IsSet("cid-source") {
			c.CidSource = ctx.String("cid-source")
		} else {
			c.CidSource = DefaultConfig.CidSource
		}
		switch c.CidSource {
		case RandomSource:
			// check the size of the random content to generate
			if ctx.IsSet("cid-content-size") {
				c.CidContentSize = ctx.Int("cid-content-size")
			} else {
				c.CidContentSize = DefaultConfig.CidContentSize
			}
			// check the number of random CIDs that we want to generate
			if ctx.IsSet("cid-number") {
				c.CidNumber = ctx.Int("cid-number")
			} else {
				c.CidNumber = DefaultConfig.CidNumber
			}
			// batch of CIDs for the entire study
			if ctx.IsSet("workers") {
				c.Workers = ctx.Int("workers")
			} else {
				c.Workers = DefaultConfig.Workers
			}
			//TODO support different types of cid files
		case TextFileSource:
			if ctx.IsSet("cid-file") {
				c.CidFile = ctx.String("cid-file")
			} else {
				c.CidFile = DefaultConfig.CidFile
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
