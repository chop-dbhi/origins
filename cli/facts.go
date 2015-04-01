package main

import (
	"fmt"
	"io"
	"os"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/view"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var factsCmd = &cobra.Command{
	Use: "facts <domain> [<entity>]",

	Short: "Output facts for a domain.",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Usage()
			os.Exit(1)
		}

		store := initStore()
		defer store.Close()

		var (
			w        io.Writer
			fr       fact.Reader
			fw       fact.Writer
			min, max int64
			err      error

			smin   = viper.GetString("facts.min")
			smax   = viper.GetString("facts.max")
			file   = viper.GetString("facts.file")
			format = viper.GetString("facts.format")
		)

		min, err = fact.ParseTime(smin)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		max, err = fact.ParseTime(smax)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Initialize domain view
		v := view.Range(store, min, max)
		dv := v.Domain(args[0])

		if len(args) > 1 {
			eid := args[1]

			fr = dv.FilteredReader(func(f *fact.Fact) bool {
				return f.Entity.Local == eid
			})
		} else {
			fr = dv.Reader()
		}

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
			fw = fact.CSVWriter(w)
		case "jsonstream":
			fw = fact.JSONStreamWriter(w)
		default:
			fmt.Printf("Unknown format %s\n", format)
			os.Exit(1)
		}

		n, err := fact.WriteReader(fr, fw)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Fprintf(os.Stderr, "%d facts\n", n)
	},
}

func init() {
	flags := factsCmd.Flags()

	flags.String("min", "", "The min tranaction time of facts to read.")
	flags.String("max", "", "The max tranaction time of facts to read.")
	flags.String("file", "", "Path to a file to write the facts to.")
	flags.String("format", "csv", "The output format of the facts.")

	viper.BindPFlag("facts.min", flags.Lookup("min"))
	viper.BindPFlag("facts.max", flags.Lookup("max"))
	viper.BindPFlag("facts.file", flags.Lookup("file"))
	viper.BindPFlag("facts.format", flags.Lookup("format"))
}
