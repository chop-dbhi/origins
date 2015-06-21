package main

import (
	"os"
	"sort"

	"github.com/chop-dbhi/origins/transactor"
	"github.com/chop-dbhi/origins/view"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var domainsCmd = &cobra.Command{
	Use: "domains",

	Short: "Outputs a list of domains.",

	Run: func(cmd *cobra.Command, args []string) {
		engine := initStorage()

		log, err := view.OpenLog(engine, transactor.DomainsDomain, "commit")

		if err != nil {
			logrus.Fatal(err)
		}

		idents, err := view.Entities(log.Now())

		if err != nil {
			logrus.Fatal(err)
		}

		sort.Sort(idents)

		for _, ident := range idents {
			os.Stdout.Write([]byte(ident.Name + "\n"))
		}
	},
}
