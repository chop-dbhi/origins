// The memory package implements an in-memory storage engine. This is primarily
// for testing.
package memory

import (
	"sync"

	"github.com/chop-dbhi/origins/engine"
)

// Engine is an in-memory store that keeps data in keyed parts.
type Engine struct {
	parts map[string]map[string][]byte

	sync.Mutex
}

func (e *Engine) Get(p, k string) ([]byte, error) {
	e.Lock()
	defer e.Unlock()

	if b, ok := e.parts[p]; ok {
		return b[k], nil
	}

	return nil, nil
}

func (e *Engine) Set(p, k string, v []byte) error {
	e.Lock()
	defer e.Unlock()

	var (
		ok bool
		b  map[string][]byte
	)

	if b, ok = e.parts[p]; !ok {
		b = make(map[string][]byte)
		e.parts[p] = b
	}

	b[k] = v

	return nil
}

func (e *Engine) Incr(p, k string) (uint64, error) {
	e.Lock()
	defer e.Unlock()

	var (
		ok bool
		id uint64
		v  []byte
		b  map[string][]byte
	)

	if b, ok = e.parts[p]; !ok {
		b = make(map[string][]byte)
		e.parts[p] = b
	}

	if v, ok = b[k]; ok {
		id = engine.DecodeCounter(v)
	}

	id++

	b[k] = engine.EncodeCounter(id)

	return id, nil
}

func (e *Engine) SetMany(c engine.Batch) error {
	e.Lock()
	defer e.Unlock()

	var (
		ok bool
		b  map[string][]byte
	)

	for k, v := range c {
		if b, ok = e.parts[k[0]]; !ok {
			b = make(map[string][]byte)
			e.parts[k[0]] = b
		}

		b[k[1]] = v
	}

	return nil
}

// Open initializes a new Engine and returns it.
func Init(opts engine.Options) (*Engine, error) {
	e := Engine{
		parts: make(map[string]map[string][]byte),
	}

	return &e, nil
}
