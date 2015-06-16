package origins

import (
	"fmt"

	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/storage/boltdb"
	"github.com/chop-dbhi/origins/storage/memory"
)

// Registered storage engines with their initializers. Aliases
// are supported as separate entries.
var StorageEngines = map[string]storage.Initializer{
	"bolt":   boltdb.Init,
	"boltdb": boltdb.Init,
	"mem":    memory.Init,
	"memory": memory.Init,
}

// Init initializes a store with the specified storage and options.
func Init(name string, opts *storage.Options) (storage.Engine, error) {
	var (
		ok     bool
		err    error
		engine storage.Engine
		init   storage.Initializer
	)

	if init, ok = StorageEngines[name]; !ok {
		return nil, fmt.Errorf("storage: unknown storage storage %s", name)
	}

	if opts == nil {
		opts = &storage.Options{}
	}

	// Copy the storage options.
	if engine, err = init(*opts); err != nil {
		return nil, fmt.Errorf("storage: %s: %s", name, err)
	}

	return engine, nil
}
