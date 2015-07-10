package main

import (
	"path/filepath"

	"github.com/Sirupsen/logrus"
	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Augments a command's flag set with storage-related flags.
func addStorageFlags(flags *pflag.FlagSet, prefix string) {
	flags.String("storage", "memory", "Storage backend. Choices are: boltdb and memory.")
	flags.String("path", "", "Path to a file or directory filesystem-based storage backends.")

	viper.BindPFlag(prefix+"_storage", flags.Lookup("storage"))
	viper.BindPFlag(prefix+"_path", flags.Lookup("path"))
}

// Commands can call this if it requires use of the store.
func initStorage(prefix string) storage.Engine {
	// Name of the storage engine.
	name := viper.GetString(prefix + "_storage")

	// Directory of the config file. Ensure the storage engine
	// path is resolved relative to the config file.
	cf := viper.ConfigFileUsed()
	dir := filepath.Dir(cf)

	// Get path relative to config file.
	path := filepath.Join(dir, viper.GetString(prefix+"_path"))

	// Supported options.
	opts := storage.Options{
		"path": path,
	}

	// Initialize the storage engine.
	engine, err := origins.Init(name, &opts)

	if err != nil {
		logrus.Fatal("storage:", err)
	}

	return engine
}
