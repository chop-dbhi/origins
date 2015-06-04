package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/view"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var logCmd = &cobra.Command{
	Use: "log <domain>",

	Short: "Output the log for a domain.",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Usage()
			os.Exit(1)
		}

		var (
			w           io.Writer
			err         error
			fw          origins.Writer
			asof, since time.Time

			domain = args[0]
			file   = viper.GetString("log_file")
			format = viper.GetString("log_format")
		)

		engine := initStorage()

		log, err := view.OpenLog(engine, domain, "log.commit")

		if err != nil {
			logrus.Fatal(err)
		}

		since, _ = chrono.Parse(viper.GetString("log_since"))
		asof, _ = chrono.Parse(viper.GetString("log_asof"))

		iter := log.Iter(since, asof)

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
			fw = origins.CSVWriter(w)
		default:
			logrus.Fatal("unknown format", format)
		}

		n, err := origins.ReadWriter(iter, fw)

		if err != nil {
			logrus.Fatal(err)
		}

		fmt.Fprintf(os.Stderr, "%d facts\n", n)
	},
}

func init() {
	flags := logCmd.Flags()

	flags.String("asof", "", "Defines the upper time boundary of facts to be read.")
	flags.String("since", "", "Defines the lower time boundary of facts to be read. ")
	flags.String("file", "", "Path to a file to write the log to.")
	flags.String("format", "csv", "The output format of the log.")

	viper.BindPFlag("log_asof", flags.Lookup("asof"))
	viper.BindPFlag("log_since", flags.Lookup("since"))
	viper.BindPFlag("log_file", flags.Lookup("file"))
	viper.BindPFlag("log_format", flags.Lookup("format"))
}
