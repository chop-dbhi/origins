package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// The main command describes the service and defaults to printing the
// help message.
var mainCmd = &cobra.Command{
	Use: "origins",

	Short: "Origins",

	Long: "Origins command line interface (CLI).",

	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func main() {
	mainCmd.AddCommand(versionCmd)
	mainCmd.AddCommand(transactCmd)

	viper.SetEnvPrefix("ORIGINS")
	viper.AutomaticEnv()

	// Default locations for the origins config file.
	viper.SetConfigName("origins")

	// Directory the program is being called from
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))

	if err == nil {
		viper.AddConfigPath(dir)
	}

	flags := mainCmd.PersistentFlags()

	flags.String("loglevel", "info", "Level of log messages to emit. Choices are: debug, info, warn, error, fatal, panic.")
	flags.String("storage", "", "Storage backend. Choices are: boltdb and memory.")
	flags.String("path", "", "Path to a file or directory filesystem-based storage backends.")
	flags.String("config", "", "Path to config file. Defaults to a origins.{json,yml,yaml} in the current working directory.")

	viper.BindPFlag("loglevel", flags.Lookup("loglevel"))
	viper.BindPFlag("storage", flags.Lookup("storage"))
	viper.BindPFlag("path", flags.Lookup("path"))
	viper.BindPFlag("config", flags.Lookup("config"))

	// Turn on debugging for all commands.
	mainCmd.ParseFlags(os.Args)

	level, err := logrus.ParseLevel(viper.GetString("loglevel"))

	if err != nil {
		fmt.Println("Invalid loglevel choice.")
		mainCmd.Help()
	}

	logrus.SetLevel(level)

	config := viper.GetString("config")

	if config != "" {
		viper.SetConfigFile(config)
	}

	// Read configuration file if present.
	viper.ReadInConfig()

	mainCmd.Execute()
}
