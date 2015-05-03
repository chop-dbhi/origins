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

func loadFile(store *storage.Store, r io.Reader, format, compression string) {
	var (
		err error

		domain = viper.GetString("load_domain")
		fake   = viper.GetBool("load_fake")
		strict = viper.GetBool("load_strict")
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

		format := viper.GetString("load_format")
		compression := viper.GetString("load_compression")

		// No path provided, use stdin.
		if len(args) == 0 {
			loadFile(store, os.Stdin, format, compression)
			return
		}

		for _, fn := range args {
			// Reset to initial value
			format = viper.GetString("load_format")
			compression = viper.GetString("load_compression")

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

			loadFile(store, file, format, compression)
		}
	},
}

func init() {
	flags := loadCmd.Flags()

	flags.String("format", "", "Format of the facts being written to the store. Choices are: csv, jsonstream")
	flags.String("compression", "", "Compression method used on the facts being loaded to the store. Choices are: bzip2, gzip")
	flags.String("domain", "", "Domain to load the facts to. If not supplied, the fact domain attribute must be defined.")
	flags.Bool("fake", false, "Set to prevent data from being written to the store.")
	flags.Bool("strict", false, "When true and a default domain is specified, the fact domain must match.")

	viper.BindPFlag("load_format", flags.Lookup("format"))
	viper.BindPFlag("load_compression", flags.Lookup("compression"))
	viper.BindPFlag("load_domain", flags.Lookup("domain"))
	viper.BindPFlag("load_fake", flags.Lookup("fake"))
	viper.BindPFlag("load_strict", flags.Lookup("strict"))
}
