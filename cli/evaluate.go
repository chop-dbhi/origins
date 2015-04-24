package main

import (
	"fmt"
	"io"
	"os"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func evaluateFile(store *storage.Store, r io.Reader) {
	var (
		format = viper.GetString("evaluate_format")
		domain = viper.GetString("evaluate_domain")
		strict = viper.GetBool("evaluate_strict")
	)

	// Wrap in a reader to handle carriage returns.
	r = &origins.UniversalReader{r}

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

	facts, err := transactor.Evaluate(store, reader, domain, strict)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fr := fact.NewReader(facts)
	fw := fact.CSVWriter(os.Stdout)

	n, err := fact.ReadWriter(fr, fw)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "%d facts\n", n)
}

var evaluateCmd = &cobra.Command{
	Use: "evaluate [path, ...]",

	Short: "Evaluates facts against the database.",

	Long: `evaluate reads facts from stdin or one or more paths and evaluates them against the database.
	The result are facts in their final state prior to being written to the store.`,

	Run: func(cmd *cobra.Command, args []string) {
		store := initStore()

		// No path provided, use stdin.
		if len(args) == 0 {
			evaluateFile(store, os.Stdin)
			return
		}

		for _, fn := range args {
			file, err := os.Open(fn)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer file.Close()

			evaluateFile(store, file)
		}
	},
}

func init() {
	flags := evaluateCmd.Flags()

	flags.String("format", "csv", "Format of the facts being written to the store. Choices are: csv, jsonstream")
	flags.String("domain", "", "Domain to evaluate the facts to. If not supplied, the fact domain attribute must be defined.")
	flags.Bool("strict", false, "When true and a default domain is specified, the fact domain must match.")

	viper.BindPFlag("evaluate_format", flags.Lookup("format"))
	viper.BindPFlag("evaluate_domain", flags.Lookup("domain"))
	viper.BindPFlag("evaluate_strict", flags.Lookup("strict"))
}
