package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
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
		pcmd.Stdout = os.Stdout
		pcmd.Stderr = os.Stderr

		// Stderr is already streamed, so the error is only used to flag a
		// non-zero exit code.
		if err := pcmd.Run(); err != nil {
			os.Exit(1)
		}
	},
}
