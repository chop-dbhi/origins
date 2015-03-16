package main

import (
	"fmt"
	"os"

	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/boltdb"
	"github.com/chop-dbhi/origins/storage/disk"
	"github.com/chop-dbhi/origins/storage/memory"
	"github.com/chop-dbhi/origins/storage/sqlite"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	version = "0.1.0"
	title   = "Origins CLI"
)

// The main command describes the service and defaults to printing the
// help message.
var mainCmd = &cobra.Command{
	Use:   "origins",
	Short: title,
	Long:  "Origins command line interface (CLI).",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

// Commands can call this if it requires use of the store.
func initStore() *storage.Store {
	var (
		err    error
		store  *storage.Store
		engine storage.Engine
	)

	opts := storage.Options{
		Path: viper.GetString("path"),
	}

	switch viper.GetString("storage") {
	case "diskv":
		engine, err = disk.Open(&opts)
	case "boltdb":
		engine, err = boltdb.Open(&opts)
	case "sqlite":
		engine, err = sqlite.Open(&opts)
	case "memory":
		engine, err = memory.Open(&opts)
	default:
		logrus.Fatal("no storage selected")
	}

	if err != nil {
		logrus.Fatal(err)
	}

	// Initialize a store.
	store, err = storage.Init(&storage.Config{
		Engine: engine,
	})

	if err != nil {
		logrus.Fatal(err)
	}

	return store
}

func main() {
	mainCmd.AddCommand(versionCmd)
	mainCmd.AddCommand(writeCmd)
	mainCmd.AddCommand(statsCmd)

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
