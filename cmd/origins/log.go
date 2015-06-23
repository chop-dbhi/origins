package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/view"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func concatDomains(engine storage.Engine, w origins.Writer, domains []string, since, asof time.Time) int {
	var (
		err      error
		n, count int
		log      *view.Log
	)

	// Output facts for each domain in the order they are supplied.
	for _, d := range domains {
		log, err = view.OpenLog(engine, d, "commit")

		if err != nil {
			logrus.Fatal(err)
		}

		v := log.View(since, asof)

		n, err = origins.Copy(v, w)

		if err != nil {
			logrus.Fatal(err)
		}

		count += n
	}

	return count
}

func mergeDomains(engine storage.Engine, w origins.Writer, domains []string, since, asof time.Time) int {
	var (
		err   error
		count int
		log   *view.Log
	)

	iters := make([]origins.Iterator, len(domains))

	// Merge and output facts across domains.
	for i, d := range domains {
		log, err = view.OpenLog(engine, d, "commit")

		if err != nil {
			logrus.Fatal(err)
		}

		iters[i] = log.View(since, asof)
	}

	if count, err = origins.Copy(view.Merge(iters...), w); err != nil {
		logrus.Fatal(err)
	}

	return count
}

var logCmd = &cobra.Command{
	Use: "log <domain> [...]",

	Short: "Output the log for one or more domains.",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Usage()
			os.Exit(1)
		}

		var (
			w           io.Writer
			fw          origins.Writer
			asof, since time.Time

			domains = args
			file    = viper.GetString("log_file")
			format  = viper.GetString("log_format")
		)

		since, _ = chrono.Parse(viper.GetString("log_since"))
		asof, _ = chrono.Parse(viper.GetString("log_asof"))

		engine := initStorage()

		if file == "" {
			w = os.Stdout
			defer os.Stdout.Sync()
		} else {
			f, err := os.Create(file)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer f.Close()

			w = f
		}

		switch format {
		case "csv":
			fw = origins.NewCSVWriter(w)
		default:
			logrus.Fatal("unknown format", format)
		}

		var count int

		if viper.GetBool("log_merge") {
			count = mergeDomains(engine, fw, domains, since, asof)
		} else {
			count = concatDomains(engine, fw, domains, since, asof)
		}

		fmt.Fprintf(os.Stderr, "%d facts\n", count)
	},
}

func init() {
	flags := logCmd.Flags()

	addStorageFlags(flags)

	flags.String("asof", "", "Defines the upper time boundary of facts to be read.")
	flags.String("since", "", "Defines the lower time boundary of facts to be read. ")
	flags.String("file", "", "Path to a file to write the log to.")
	flags.String("format", "csv", "The output format of the log.")
	flags.Bool("merge", false, "Multiple domains will be merged.")

	viper.BindPFlag("log_asof", flags.Lookup("asof"))
	viper.BindPFlag("log_since", flags.Lookup("since"))
	viper.BindPFlag("log_file", flags.Lookup("file"))
	viper.BindPFlag("log_format", flags.Lookup("format"))
	viper.BindPFlag("log_merge", flags.Lookup("merge"))
}
