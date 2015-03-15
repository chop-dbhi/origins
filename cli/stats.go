package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/chop-dbhi/origins/view"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var statsCmd = &cobra.Command{
	Use: "stats <domain>",

	Short: "Print stats for a domain.",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
		}

		store := initStore()

		var (
			err error
			min = viper.GetInt("min")
			max = viper.GetInt("max")
		)

		// Current view of the store.
		v, err := view.Range(store, min, max)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// View of the passed domain.
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

	viper.BindPFlag("min", flags.Lookup("min"))
	viper.BindPFlag("max", flags.Lookup("max"))
}
