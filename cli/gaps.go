package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/chop-dbhi/origins/view"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var gapsCmd = &cobra.Command{
	Use: "gaps <domain>",

	Short: "Print a list of entity/attribute pairs with gaps in a domain.",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Usage()
			os.Exit(1)
		}

		s := viper.GetString("gaps_threshold")
		d, err := time.ParseDuration(s)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing threshold as a duration.")
			cmd.Usage()
			os.Exit(1)
		}

		store := initStore()

		v := view.Now(store)
		dv := v.Domain(args[0])

		var (
			b []byte
		)

		for _, gs := range dv.Gaps(d) {
			b, err = json.MarshalIndent(gs, "", "\t")

			if err != nil {
				logrus.Error(err)
				continue
			}

			fmt.Println(string(b))
		}
	},
}

func init() {
	flags := gapsCmd.Flags()

	flags.String("threshold", "1s", "The maximum duration allowed between a retraction and assertion.")

	viper.BindPFlag("gaps_threshold", flags.Lookup("threshold"))
}
