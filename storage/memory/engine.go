/*
The memory package implements an in-memory store for testing.
*/
package memory

import "github.com/chop-dbhi/origins/storage"

// Engine is an in-memory store that keeps data in keyed bins.
type Engine struct {
	bins map[string][]byte
}

func (e *Engine) Get(k string) ([]byte, error) {
	return e.bins[k], nil
}

func (e *Engine) Set(k string, v []byte) error {
	e.bins[k] = v
	return nil
}

func (e *Engine) SetMany(b storage.Batch) error {
	for k, v := range b {
		e.bins[k] = v
	}

	return nil
}

func (e *Engine) Close() error {
	return nil
}

// Open initializes a new Engine and returns it.
func Open(opts *storage.Options) (*Engine, error) {
	e := Engine{
		bins: make(map[string][]byte),
	}

	return &e, nil
}
