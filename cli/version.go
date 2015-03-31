package main

import (
	"fmt"

	"github.com/chop-dbhi/origins"
	"github.com/spf13/cobra"
)

// The version command prints the version of the CLI.
var versionCmd = &cobra.Command{
	Use: "version",

	Short: "Print the version.",

	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(origins.Version)
	},
}
