package main

import (
	"os"
	"sort"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/view"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var domainsCmd = &cobra.Command{
	Use: "domains",

	Short: "Outputs a list of domains.",

	Run: func(cmd *cobra.Command, args []string) {
		engine := initStorage()

		log, err := view.OpenLog(engine, origins.DomainsDomain, "commit")

		if err != nil {
			logrus.Fatal(err)
		}

		idents, err := origins.Entities(log.Now())

		if err != nil {
			logrus.Fatal(err)
		}

		sort.Sort(idents)

		for _, ident := range idents {
			os.Stdout.Write([]byte(ident.Name + "\n"))
		}
	},
}

func init() {
	flags := domainsCmd.Flags()

	addStorageFlags(flags)
}
