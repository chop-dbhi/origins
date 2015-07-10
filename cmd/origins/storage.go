package main

import (
	"path/filepath"

	"github.com/Sirupsen/logrus"
	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Adds storage-related flags to a command's flag set.
func addStorageFlags(flags *pflag.FlagSet) {
	flags.String("storage", "memory", "Storage backend. Choices are: boltdb and memory.")
	flags.String("path", "", "Path to a file or directory filesystem-based storage backends.")
}

// Binds the storage flags to the viper object. This should be called within
// the command's Run method.
func bindStorageFlags(flags *pflag.FlagSet) {
	viper.BindPFlag("storage", flags.Lookup("storage"))
	viper.BindPFlag("path", flags.Lookup("path"))
}

// Commands can call this if it requires use of the store.
func initStorage() storage.Engine {
	// Name of the storage engine.
	name := viper.GetString("storage")
	path := viper.GetString("path")

	// Directory of the config file. Ensure the storage engine
	// path is resolved relative to the config file.
	dir := filepath.Dir(viper.ConfigFileUsed())
	path = filepath.Join(dir, path)

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
