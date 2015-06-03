package main

import (
	"fmt"
	"io"
	"os"

	"github.com/chop-dbhi/origins"
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
			w  io.Writer
			fw origins.Writer
			//min, max int64
			err error

			domain = args[0]

			//smin   = viper.GetString("log_min")
			//smax   = viper.GetString("log_max")
			file   = viper.GetString("log_file")
			format = viper.GetString("log_format")
		)

		engine := initStorage()

		log, err := view.OpenLog(engine, domain, "log.commit")

		if err != nil {
			logrus.Fatal(err)
		}

		iter := log.Iter()

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

	flags.String("min", "", "The min tranaction time of log to read.")
	flags.String("max", "", "The max tranaction time of log to read.")
	flags.String("file", "", "Path to a file to write the log to.")
	flags.String("format", "csv", "The output format of the log.")

	viper.BindPFlag("log_min", flags.Lookup("min"))
	viper.BindPFlag("log_max", flags.Lookup("max"))
	viper.BindPFlag("log_file", flags.Lookup("file"))
	viper.BindPFlag("log_format", flags.Lookup("format"))
}
