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

	viper.BindPFlag("loglevel", flags.Lookup("loglevel"))

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
