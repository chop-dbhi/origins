package main

import (
	"fmt"
	"io"
	"os"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func loadFile(store *storage.Store, domain string, format string, fake bool, r io.Reader) {
	// Wrap in a reader to handle carriage returns.
	r = &UniversalReader{r}

	var reader fact.Reader

	switch format {
	case "csv":
		reader = fact.CSVReader(r)
	case "jsonstream":
		reader = fact.JSONStreamReader(r)
	default:
		fmt.Printf("Unknown format %s\n", format)
		os.Exit(1)
	}

	results, err := transactor.Transact(store, reader, domain, false, !fake)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, r := range results {
		fmt.Println(r)
	}
}

var loadCmd = &cobra.Command{
	Use: "load [path, ...]",

	Short: "Loads facts into the store.",

	Long: `load takes one or more paths to read or "-" to read from stdin.`,

	Run: func(cmd *cobra.Command, args []string) {
		store := initStore()

		var (
			format = viper.GetString("load.format")
			domain = viper.GetString("load.domain")
			fake   = viper.GetBool("load.fake")
		)

		// No path provided, use stdin.
		if len(args) == 0 {
			loadFile(store, domain, format, fake, os.Stdin)
			return
		}

		for _, fn := range args {
			file, err := os.Open(fn)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer file.Close()

			loadFile(store, domain, format, fake, file)
		}
	},
}

func init() {
	flags := loadCmd.Flags()

	flags.String("format", "csv", "Format of the facts being written to the store. Choices are: csv, jsonstream")
	flags.String("domain", "", "Domain to load the facts to. If not supplied, the fact domain attribute must be defined.")
	flags.Bool("fake", false, "Set to prevent data from being written to the store.")

	viper.BindPFlag("load.format", flags.Lookup("format"))
	viper.BindPFlag("load.domain", flags.Lookup("domain"))
	viper.BindPFlag("load.fake", flags.Lookup("fake"))
}
