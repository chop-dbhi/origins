package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func transactFile(engine storage.Engine, r io.Reader, format, compression string) {
	var (
		err error

		domain = viper.GetString("transact_domain")
		fake   = viper.GetBool("transact_fake")
	)

	// Apply decompression.
	if compression != "" {
		logrus.Debugf("transact: applying %s decompression", compression)

		if r, err = origins.Decompressor(r, compression); err != nil {
			logrus.Fatalf("transact: %s", err)
		}
	}

	// Wrap in a reader to handle carriage returns before passing
	// it into the format reader.
	r = origins.NewUniversalReader(r)

	var iter origins.Iterator

	switch format {
	case "csv":
		iter = origins.CSVReader(r)
	default:
		logrus.Fatal("transact: unsupported file format", format)
	}

	tx, err := transactor.New(engine, transactor.Options{
		DefaultDomain: domain,
	})

	if err != nil {
		logrus.Fatal("transact: error starting transaction:", err)
	}

	if err = tx.AppendIter(iter); err != nil {
		logrus.Fatal("transact:", err)
	}

	if fake {
		err = tx.Cancel()
	} else {
		err = tx.Commit()
	}

	if err != nil {
		logrus.Error("transact:", err)
	} else {
		stats, _ := json.Marshal(tx.Stats())
		logrus.Info("transact:", string(stats))
	}

	logrus.Infof("transact: took %s", tx.EndTime.Sub(tx.StartTime))
}

var transactCmd = &cobra.Command{
	Use: "transact [path, ...]",

	Short: "Transacts facts into storage.",

	Long: `transact reads facts from stdin or from one or more paths specified paths.`,

	Run: func(cmd *cobra.Command, args []string) {
		engine := initStorage()

		format := viper.GetString("transact_format")
		compression := viper.GetString("transact_compression")

		// No path provided, use stdin.
		if len(args) == 0 {
			transactFile(engine, os.Stdin, format, compression)
			return
		}

		for _, fn := range args {
			// Reset to initial value
			format = viper.GetString("transact_format")
			compression = viper.GetString("transact_compression")

			if format == "" {
				format = origins.DetectFileFormat(fn)
			}

			if compression == "" {
				compression = origins.DetectFileCompression(fn)
			}

			file, err := os.Open(fn)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer file.Close()

			transactFile(engine, file, format, compression)
		}
	},
}

func init() {
	flags := transactCmd.Flags()

	flags.String("format", "", "Format of the stream of facts. Choices are: csv")
	flags.String("compression", "", "Compression method of the stream of facts. Choices are: bzip2, gzip")
	flags.String("domain", "", "Default domain to transact the facts to. If not supplied, the fact domain attribute must be defined.")
	flags.Bool("fake", false, "If set, the transaction will not be committed.")

	viper.BindPFlag("transact_format", flags.Lookup("format"))
	viper.BindPFlag("transact_compression", flags.Lookup("compression"))
	viper.BindPFlag("transact_domain", flags.Lookup("domain"))
	viper.BindPFlag("transact_fake", flags.Lookup("fake"))
}
