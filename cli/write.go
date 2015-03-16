package main

import (
	"fmt"
	"io"
	"os"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var writeCmd = &cobra.Command{
	Use: "write (<file>| -)",

	Short: "Writes facts to the store.",

	Long: `write takes a path to a file or "-" to read from stdin.`,

	Run: func(cmd *cobra.Command, args []string) {
		store := initStore()

		format := viper.GetString("format")
		domain := viper.GetString("domain")

		if len(args) == 0 {
			cmd.Help()
		}

		var r io.Reader

		if args[0] == "-" {
			r = os.Stdin
		} else {
			f, err := os.Open(args[0])

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer f.Close()

			r = f
		}

		var (
			reader fact.Reader
			err    error
		)

		switch format {
		case "csv":
			reader, err = fact.CSVReader(r)
		case "json":
			reader, err = fact.JSONStreamReader(r)
		default:
			fmt.Println("Unknown format %s", format)
		}

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		err = transactor.Transact(store, reader, domain, true, true)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	flags := writeCmd.Flags()

	flags.String("format", "csv", "The format of the facts being written to the store. Choices are: csv, json")
	flags.String("domain", "", "The domain to write the facts to. If not supplied, the fact domain attribute must be defined.")

	viper.BindPFlag("format", flags.Lookup("format"))
	viper.BindPFlag("domain", flags.Lookup("domain"))
}
