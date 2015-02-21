package storage

import (
	"errors"

	"github.com/sirupsen/logrus"
)

const (
	defaultStoreName = "origins_store"
)

var (
	ErrEngineRequired = errors.New("A storage engine required.")
)

// Config is a map structure for store options.
type Config struct {
	// Name of the store itself. Each store is isolated to this scoped to a
	// particular store name
	Name string

	// Engine is storage engine for this store.
	Engine Engine
}

// Init initializes and returns store.
func Init(cfg *Config) (*Store, error) {
	var err error

	if cfg.Name == "" {
		logrus.Infof("No store name specified, using '%v'", defaultStoreName)
		cfg.Name = defaultStoreName
	}

	if cfg.Engine == nil {
		logrus.Error(ErrEngineRequired)
		return nil, ErrEngineRequired
	}

	store := Store{
		Name:   cfg.Name,
		engine: cfg.Engine,
	}

	// Internal initialization.
	err = store.init()

	if err != nil {
		return nil, err
	}

	return &store, nil
}
