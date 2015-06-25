package main

import (
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// The generate command executes a program that generates facts form an
// external source.
var generateCmd = &cobra.Command{
	Use: "generate <name> ...",

	Short: "Generate facts from an existing data source.",

	Long: `The generate command is a proxy for executing programs that
generate facts from other programs or data sources.

A valid generator must an executable program that has the name
'origins-generate-<name>' where <name> is the shorthand name passed
to this command. The program must write all facts to stdout in the
CSV format.

A collection of generators are being maintained at:
https://github.com/chop-dbhi/origins-generators`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Usage()
			return
		}

		// Check optional arguments.
		domain := viper.GetString("generate_domain")
		ts := viper.GetString("generate_time")
		op := viper.GetString("generate_operation")

		var (
			t   time.Time
			err error
		)

		if ts != "" {
			t, err = chrono.Parse(ts)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}

		operation, err := origins.ParseOperation(op)

		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}

		// Construct the full script name.
		name := fmt.Sprintf("origins-generate-%s", args[0])

		// Find the program on $PATH.
		path, err := exec.LookPath(name)

		if err != nil {
			fmt.Printf("origins: the '%s' generator could not be found.\n", args[0])
			os.Exit(1)
		}

		// Construct the command to execute the generator.
		pcmd := exec.Command(path, args[1:]...)

		// Proxy standard interfaces.
		pcmd.Stdin = os.Stdin
		pcmd.Stderr = os.Stderr

		// Get a stdout pipe from the command for the reader to consume.
		stdout, err := pcmd.StdoutPipe()

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		var (
			iterator origins.Iterator
			writer   origins.Writer
		)

		// Data written to the buffer are converted into facts, normalized, and
		// copied to the CSV writer.
		iterator = origins.NewCSVReader(stdout)

		// Facts are written to stdout.
		writer = origins.NewCSVWriter(os.Stdout)

		// Modify the fact before writing using the passed arguments.
		// TODO: there is a overlap with the transactor.
		writer = &modWriter{
			writer: writer,
			modifier: func(f *origins.Fact) {
				if f.Domain == "" {
					f.Domain = domain
				}

				f.Operation = operation

				if f.Time.IsZero() {
					f.Time = t
				}
			},
		}

		// Start the command. Stderr is already mapped, so only the exit
		// code needs to be handled here.
		if err := pcmd.Start(); err != nil {
			os.Exit(1)
		}

		origins.Copy(iterator, writer)

		// Wait until the command exits.
		if err := pcmd.Wait(); err != nil {
			os.Exit(1)
		}
	},
}

// modWriter modifies the fact before writing it.
type modWriter struct {
	writer   origins.Writer
	modifier func(*origins.Fact)
}

func (w *modWriter) Write(f *origins.Fact) error {
	w.modifier(f)
	return w.writer.Write(f)
}

func (w *modWriter) Flush() error {
	// If the writer implements Flusher, flush it.
	switch x := w.writer.(type) {
	case origins.Flusher:
		return x.Flush()
	}

	return nil
}

func init() {
	flags := generateCmd.Flags()

	flags.String("domain", "", "Default domain to set on the facts.")
	flags.String("time", "", "The time the facts are true.")
	flags.String("operation", "", "The operation to apply to the facts, assert or retract.")

	viper.BindPFlag("generate_domain", flags.Lookup("domain"))
	viper.BindPFlag("generate_time", flags.Lookup("time"))
	viper.BindPFlag("generate_operation", flags.Lookup("operation"))
}
