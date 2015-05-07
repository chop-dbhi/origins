package main

import (
	"fmt"

	"github.com/chop-dbhi/origins/view"
	"github.com/spf13/cobra"
)

var domainsCmd = &cobra.Command{
	Use: "domains",

	Short: "Print a list of domains.",

	Run: func(cmd *cobra.Command, args []string) {
		store := initStore()

		v := view.Now(store)
		dv := v.Domain("origins.domains")

		for _, i := range dv.Entities() {
			fmt.Println(i.Local)
		}
	},
}
