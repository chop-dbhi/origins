package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

const version = "0.1.0"

// The version command prints the version of the CLI.
var versionCmd = &cobra.Command{
	Use: "version",

	Short: "Print the version.",

	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version)
	},
}
