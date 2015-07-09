package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sirupsen/logrus"
	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/transactor"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func transactFile(tx *transactor.Transaction, r io.Reader, format, compression string) {
	var (
		err error
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

	var pub origins.Publisher

	switch format {
	case "csv":
		pub = origins.NewCSVReader(r)
	default:
		logrus.Fatal("transact: unsupported file format", format)
	}

	if err = tx.Consume(pub); err != nil {
		logrus.Fatal("transact:", err)
	}
}

var transactCmd = &cobra.Command{
	Use: "transact [path, ...]",

	Short: "Transacts facts into storage.",

	Long: `transact reads facts from stdin or from one or more paths specified paths.`,

	Run: func(cmd *cobra.Command, args []string) {
		engine := initStorage("transact")

		format := viper.GetString("transact_format")
		compression := viper.GetString("transact_compression")
		domain := viper.GetString("transact_domain")
		fake := viper.GetBool("transact_fake")

		tx, err := transactor.New(engine, transactor.Options{
			DefaultDomain: domain,
		})

		if err != nil {
			logrus.Fatal("transact: error starting transaction:", err)
		}

		// Register handler to catch interrupt (ctrl+c). The response
		// cancels the transaction. A panic recovery is used since facts
		// may still be sent on the stream after the channel is closed.
		// TODO: improve the cleanup handling.
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

		go func() {
			<-sig
			tx.Cancel()
		}()

		defer func() {
			recover()
		}()

		// No path provided, use stdin.
		if len(args) == 0 {
			transactFile(tx, os.Stdin, format, compression)
		} else {
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

				transactFile(tx, file, format, compression)
			}
		}

		if fake {
			tx.Cancel()
		} else {
			err = tx.Commit()
		}

		if err != nil {
			logrus.Fatalf("transact: %s", err)
		}

		info, _ := json.MarshalIndent(tx.Info(), "", "\t")

		fmt.Println(string(info))
	},
}

func init() {
	flags := transactCmd.Flags()

	addStorageFlags(flags, "transact")

	flags.String("format", "", "Format of the stream of facts. Choices are: csv")
	flags.String("compression", "", "Compression method of the stream of facts. Choices are: bzip2, gzip")
	flags.String("domain", "", "Default domain to transact the facts to. If not supplied, the fact domain attribute must be defined.")
	flags.Bool("fake", false, "If set, the transaction will not be committed.")

	viper.BindPFlag("transact_format", flags.Lookup("format"))
	viper.BindPFlag("transact_compression", flags.Lookup("compression"))
	viper.BindPFlag("transact_domain", flags.Lookup("domain"))
	viper.BindPFlag("transact_fake", flags.Lookup("fake"))
}
