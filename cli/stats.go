package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/view"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var statsCmd = &cobra.Command{
	Use: "stats <domain>",

	Short: "Print stats for a domain.",

	Run: func(cmd *cobra.Command, args []string) {
		// Domain name is required.
		if len(args) == 0 {
			cmd.Usage()
			os.Exit(1)
		}

		store := initStore()

		var (
			err      error
			min, max int64

			smin = viper.GetInt("stats.min")
			smax = viper.GetInt("stats.max")
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

		// View for the domain.
		v := view.Range(store, min, max)
		dv := v.Domain(args[0])

		stats := dv.Stats()

		b, _ := json.MarshalIndent(&stats, "", "\t")
		fmt.Println(string(b))
	},
}

func init() {
	flags := statsCmd.Flags()

	flags.Int64("min", 0, "The min time of the view.")
	flags.Int64("max", 0, "The max time of the view.")

	viper.BindPFlag("stats.min", flags.Lookup("min"))
	viper.BindPFlag("stats.max", flags.Lookup("max"))
}
