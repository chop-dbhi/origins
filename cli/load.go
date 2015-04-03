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

func loadFile(store *storage.Store, r io.Reader) {

	var (
		err error

		format = viper.GetString("load.format")
		domain = viper.GetString("load.domain")
		fake   = viper.GetBool("load.fake")
		strict = viper.GetBool("load.strict")
	)

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

	var results transactor.Results

	if fake {
		results, err = transactor.Test(store, reader, domain, strict)
	} else {
		results, err = transactor.Commit(store, reader, domain, strict)
	}

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

	Long: `load reads facts from stdin or from one or more paths specified paths.`,

	Run: func(cmd *cobra.Command, args []string) {
		store := initStore()

		// No path provided, use stdin.
		if len(args) == 0 {
			loadFile(store, os.Stdin)
			return
		}

		for _, fn := range args {
			file, err := os.Open(fn)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer file.Close()

			loadFile(store, file)
		}
	},
}

func init() {
	flags := loadCmd.Flags()

	flags.String("format", "csv", "Format of the facts being written to the store. Choices are: csv, jsonstream")
	flags.String("domain", "", "Domain to load the facts to. If not supplied, the fact domain attribute must be defined.")
	flags.Bool("fake", false, "Set to prevent data from being written to the store.")
	flags.Bool("strict", false, "When true and a default domain is specified, the fact domain must match.")

	viper.BindPFlag("load.format", flags.Lookup("format"))
	viper.BindPFlag("load.domain", flags.Lookup("domain"))
	viper.BindPFlag("load.fake", flags.Lookup("fake"))
	viper.BindPFlag("load.strict", flags.Lookup("strict"))
}
