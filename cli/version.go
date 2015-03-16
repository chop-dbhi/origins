package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// The version command prints this service.
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version)
	},
}
