package main

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func evaluateFile(store *storage.Store, r io.Reader, format, compression string) {
	var (
		err error

		domain = viper.GetString("evaluate_domain")
		strict = viper.GetBool("evaluate_strict")
	)

	// Apply decompression.
	if compression != "" {
		logrus.Debugf("Applying %s decompression", compression)

		switch compression {
		case "bzip2":
			r = bzip2.NewReader(r)
		case "gzip":
			if r, err = gzip.NewReader(r); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		default:
			fmt.Printf("Unsupported compression format %s\n", compression)
			os.Exit(1)
		}
	}

	// Wrap in a reader to handle carriage returns before passing
	// it into the CSV reader.
	r = &origins.UniversalReader{r}

	var reader fact.Reader

	switch format {
	case "csv":
		reader = fact.CSVReader(r)
	case "jsonstream", "jsons":
		reader = fact.JSONStreamReader(r)
	default:
		fmt.Printf("Unsupported file format %s\n", format)
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

		format := viper.GetString("evaluate_format")
		compression := viper.GetString("evaluate_compression")

		// No path provided, use stdin.
		if len(args) == 0 {
			evaluateFile(store, os.Stdin, format, compression)
			return
		}

		for _, fn := range args {
			// Reset to initial value
			format = viper.GetString("evaluate_format")
			compression = viper.GetString("evaluate_compression")

			if format == "" {
				format = origins.DetectFormat(fn)
			}

			if compression == "" {
				compression = origins.DetectCompression(fn)
			}

			file, err := os.Open(fn)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer file.Close()

			evaluateFile(store, file, format, compression)
		}
	},
}

func init() {
	flags := evaluateCmd.Flags()

	flags.String("format", "", "Format of the facts being evaluated. Choices are: csv, jsonstream")
	flags.String("compression", "", "Compression method used on the facts being evaluated. Choices are: bzip2, gzip")
	flags.String("domain", "", "Domain to evaluate the facts against. If not supplied, the fact domain attribute must be defined.")
	flags.Bool("strict", false, "When true and a default domain is specified, the fact domain must match.")

	viper.BindPFlag("evaluate_format", flags.Lookup("format"))
	viper.BindPFlag("evaluate_compression", flags.Lookup("compression"))
	viper.BindPFlag("evaluate_domain", flags.Lookup("domain"))
	viper.BindPFlag("evaluate_strict", flags.Lookup("strict"))
}
