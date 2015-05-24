// The memory package implements an in-memory storage storage. This is primarily
// for testing or one-off analyses.
package memory

import (
	"sync"

	"github.com/chop-dbhi/origins/storage"
)

type Tx struct {
	e *Engine
}

func (t *Tx) Get(p, k string) ([]byte, error) {
	if b, ok := t.e.parts[p]; ok {
		return b[k], nil
	}

	return nil, nil
}

func (t *Tx) Set(p, k string, v []byte) error {
	var (
		ok bool
		b  map[string][]byte
	)

	if b, ok = t.e.parts[p]; !ok {
		b = make(map[string][]byte)
		t.e.parts[p] = b
	}

	b[k] = v

	return nil
}

func (t *Tx) Delete(p, k string) error {
	if m, ok := t.e.parts[p]; ok {
		delete(m, k)
	}

	return nil
}

func (t *Tx) Incr(p, k string) (uint64, error) {
	var (
		ok bool
		id uint64
		v  []byte
		b  map[string][]byte
	)

	if b, ok = t.e.parts[p]; !ok {
		b = make(map[string][]byte)
		t.e.parts[p] = b
	}

	if v, ok = b[k]; ok {
		id = storage.DecodeCounter(v)
	}

	id++

	b[k] = storage.EncodeCounter(id)

	return id, nil
}

// Engine is an in-memory store that keeps data in keyed parts.
type Engine struct {
	parts map[string]map[string][]byte

	sync.Mutex
}

func (e *Engine) Get(p, k string) ([]byte, error) {
	e.Lock()
	defer e.Unlock()

	t := &Tx{e}

	return t.Get(p, k)
}

func (e *Engine) Set(p, k string, v []byte) error {
	e.Lock()
	defer e.Unlock()

	t := &Tx{e}

	return t.Set(p, k, v)
}

func (e *Engine) Delete(p, k string) error {
	e.Lock()
	defer e.Unlock()

	t := &Tx{e}

	return t.Delete(p, k)
}

func (e *Engine) Incr(p, k string) (uint64, error) {
	e.Lock()
	defer e.Unlock()

	t := &Tx{e}

	return t.Incr(p, k)
}

func (e *Engine) Multi(f func(tx storage.Tx) error) error {
	e.Lock()
	defer e.Unlock()

	return f(&Tx{e})
}

// Open initializes a new Engine and returns it.
func Init(opts storage.Options) (storage.Engine, error) {
	e := Engine{
		parts: make(map[string]map[string][]byte),
	}

	return &e, nil
}
