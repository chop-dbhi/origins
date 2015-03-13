package main

import (
	"fmt"
	"io"
	"os"

	"github.com/chop-dbhi/origins/fact"
	"github.com/spf13/cobra"
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
			n   int
			err error
		)

		r, err := store.Reader(args[0])

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		total := 0
		buf := make(fact.Facts, 100)

		for {
			n, err = r.Read(buf)

			if err == io.EOF {
				total += n
				break
			}

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			total += n
		}

		fmt.Printf("Total facts: %d\n", total)
	},
}
