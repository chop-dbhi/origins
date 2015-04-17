package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/view"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var aggregateCmd = &cobra.Command{
	Use: "aggregate <domain> <id>",

	Short: "Outputs an aggregate of the domain identifier.",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			cmd.Usage()
			os.Exit(1)
		}

		var (
			err      error
			min, max int64

			smin = viper.GetString("aggregate_min")
			smax = viper.GetString("aggregate_max")
		)

		min, err = fact.ParseTime(smin)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		max, err = fact.ParseTime(smax)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Domain to read from.
		domain := args[0]
		id := args[1]

		store := initStore()

		v := view.Range(store, min, max)
		dv := v.Domain(domain)

		ident, err := identity.Parse(id)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		agg := dv.Aggregate(ident)

		b, err := json.MarshalIndent(agg.Map(), "", "\t")

		if err != nil {
			logrus.Fatal(err)
		}

		fmt.Println(string(b))
	},
}

func init() {
	flags := aggregateCmd.Flags()

	flags.String("min", "", "The min tranaction time of facts to read.")
	flags.String("max", "", "The max tranaction time of facts to read.")

	viper.BindPFlag("aggregate_min", flags.Lookup("min"))
	viper.BindPFlag("aggregate_max", flags.Lookup("max"))
}
