package main

import (
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/boltdb"
	"github.com/chop-dbhi/origins/storage/disk"
	"github.com/chop-dbhi/origins/storage/memory"
	"github.com/chop-dbhi/origins/storage/sqlite"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

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
