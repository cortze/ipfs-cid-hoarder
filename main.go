package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cortze/ipfs-cid-hoarder/cmd"

	"github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

var (
	CliName    = "ipfs-cid-hoarder"
	CliVersion = "v0.1.0"
	log        = logrus.WithField(
		"App", CliName,
	)
)

func main() {
	fmt.Println(CliName, CliVersion, "\n")

	cidHoarder := cli.App{
		Name:      CliName,
		Usage:     "Tiny cli to request CIDs in the IPFS network with the double task of analyze their metadata and availability",
		UsageText: "cid-hoarder [subcommands] [arguments]",
		Authors: []*cli.Author{
			{
				Name:  "Mikel Cortes (@cortze)",
				Email: "cortze@protonmail.com",
			},
		},
		Commands: []*cli.Command{
			cmd.RunCmd,
		},
	}

	if err := cidHoarder.RunContext(context.Background(), os.Args); err != nil {
		log.Errorf("error: %v\n", err)
		os.Exit(1)
	}
}
