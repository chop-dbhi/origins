// The boltdb package implements a storage engine that interfaces with BoltDB.
package boltdb

import (
	"errors"

	"github.com/boltdb/bolt"
	"github.com/chop-dbhi/origins/engine"
)

var (
	ErrPathRequired = errors.New("Path to the boltdb file required")
)

type Engine struct {
	Path string
}

func (e *Engine) Get(p, k string) ([]byte, error) {
	db, err := bolt.Open(e.Path, 0600, nil)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	var (
		s, v   []byte
		exists = true
	)

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(p))

		// No bucket.
		if b == nil {
			exists = false
			return nil
		}

		// Get the value and copy it into a new buffer.
		s = b.Get([]byte(k))

		v = make([]byte, len(s))
		copy(v, s)

		return nil
	})

	if err != nil {
		return nil, err
	}

	if exists {
		return v, nil
	}

	return nil, nil
}

func (e *Engine) Incr(p, k string) (uint64, error) {
	db, err := bolt.Open(e.Path, 0600, nil)

	if err != nil {
		return 0, err
	}

	defer db.Close()

	var (
		id  uint64
		buf []byte
	)

	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(p))

		if err != nil {
			return err
		}

		key := []byte(k)

		buf = b.Get(key)

		if buf != nil {
			id = engine.DecodeCounter(buf)
		}

		id++

		return b.Put(key, engine.EncodeCounter(id))
	})

	if err != nil {
		return 0, err
	}

	return id, nil
}

func (e *Engine) Set(p, k string, v []byte) error {
	db, err := bolt.Open(e.Path, 0600, nil)

	if err != nil {
		return err
	}

	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(p))

		if err != nil {
			return err
		}

		return b.Put([]byte(k), v)
	})
}

func (e *Engine) SetMany(c engine.Batch) error {
	db, err := bolt.Open(e.Path, 0600, nil)

	if err != nil {
		return err
	}

	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		// Key is a 2L array
		for k, v := range c {
			b, err := tx.CreateBucketIfNotExists([]byte(k[0]))

			if err != nil {
				return err
			}

			if err = b.Put([]byte(k[1]), v); err != nil {
				return err
			}
		}

		return nil
	})
}

func Init(opts engine.Options) (*Engine, error) {
	path := opts.GetString("path")

	if path == "" {
		return nil, ErrPathRequired
	}

	e := Engine{
		Path: path,
	}

	return &e, nil
}
