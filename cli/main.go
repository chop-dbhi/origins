package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// The main command describes the service and defaults to printing the
// help message.
var mainCmd = &cobra.Command{
	Use: "origins",

	Short: "Origins CLI",

	Long: "Origins command line interface (CLI).",

	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func main() {
	mainCmd.AddCommand(versionCmd)
	mainCmd.AddCommand(loadCmd)
	mainCmd.AddCommand(statsCmd)
	mainCmd.AddCommand(factsCmd)

	viper.SetEnvPrefix("ORIGINS_CLI")
	viper.AutomaticEnv()

	flags := mainCmd.PersistentFlags()

	flags.String("loglevel", "info", "Level of log messages to emit. Choices are: debug, info, warn, error, fatal, panic.")
	flags.String("path", "", "Path to a file or directory filesystem-based storage backends.")
	flags.String("storage", "", "Storage backend. Choices are: boltdb, diskv, sqlite, and memory.")

	viper.BindPFlag("loglevel", flags.Lookup("loglevel"))
	viper.BindPFlag("path", flags.Lookup("path"))
	viper.BindPFlag("storage", flags.Lookup("storage"))

	// Turn on debugging for all commands.
	mainCmd.ParseFlags(os.Args)

	level, err := logrus.ParseLevel(viper.GetString("loglevel"))

	if err != nil {
		fmt.Println("Invalid loglevel choice.")
		mainCmd.Help()
	}

	logrus.SetLevel(level)

	mainCmd.Execute()
}
