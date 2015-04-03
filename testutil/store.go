package testutil

import (
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/memory"
)

func Store() *storage.Store {
	engine, _ := memory.Open(&storage.Options{})

	store, _ := storage.Init(&storage.Config{
		Engine: engine,
	})

	return store
}
