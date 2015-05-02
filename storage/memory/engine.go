/*
The memory package implements an in-memory store for testing.
*/
package memory

import (
	"encoding/binary"
	"errors"

	"github.com/chop-dbhi/origins/storage"
)

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

func (e *Engine) Incr(k string) (uint64, error) {
	var (
		ok   bool
		id   uint64
		buf  []byte
		erri int
	)

	if buf, ok = e.bins[k]; ok {
		id, erri = binary.Uvarint(buf)

		if erri == 0 {
			return 0, errors.New("memory: buffer too small for value")
		} else if erri < 0 {
			return 0, errors.New("memory: value larger than 64 bits")
		}
	}

	buf = make([]byte, 8)

	id += 1

	n := binary.PutUvarint(buf, id)
	e.bins[k] = buf[:n]

	return id, nil
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
