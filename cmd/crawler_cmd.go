package cmd

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/cortze/ipfs-cid-hoarder/pkg/config"
	"github.com/cortze/ipfs-cid-hoarder/pkg/crawler"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

var CrawlerCmd = &cli.Command{
	Name:   "crawler",
	Usage:  "starts simple crawl over the IPFS Network",
	Action: RunCrawler,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:        "log-level",
			Usage:       "verbosity of the logs that will be displayed [debug,warn,info,error]",
			EnvVars:     []string{"IPFS_CID_HOARDER_LOGLEVEL"},
			DefaultText: "info",
			Value:       config.DefaultConfig.LogLevel,
		},
		&cli.StringFlag{
			Name:        "blacklisting-user-agent",
			Usage:       "user agent that wants to be blacklisted as fruit of the crawl",
			EnvVars:     []string{"IPFS_CID_HOARDER_BLOCKLIST_USER_AGENT"},
			DefaultText: config.DefaultBlacklistUserAgent,
			Value:       config.DefaultBlacklistUserAgent,
		},
	},
}

func RunCrawler(ctx *cli.Context) error {
	// here goes all the magic

	// generate config from the urfave/cli context
	conf, err := config.NewConfig(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to generate config from arguments")
	}

	term, _ := config.ParseLogOutput("terminal")

	// set the logs configurations
	log.SetFormatter(config.ParseLogFormatter("text"))
	log.SetOutput(term)
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

	var blacklistUserAgent string

	// check if a blacklisting user agent has been submitted
	if ctx.IsSet("blacklisting-user-agent") {
		blacklistUserAgent = ctx.String("blacklisting-user-agent")
	}

	_, err = crawler.RunLightCrawler(ctx.Context, blacklistUserAgent)
	if err != nil {
		return err
	}
	return nil
}
