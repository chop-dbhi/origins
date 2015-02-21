/*
The disk package wraps the Diskv (https://github.com/peterbourgon/diskv) file-based
key/value store.
*/
package boltdb

import (
	"errors"

	"github.com/boltdb/bolt"
	"github.com/chop-dbhi/origins/storage"
)

var (
	ErrPathRequired = errors.New("Path to the boltdb file required")
)

type Engine struct {
	db     *bolt.DB
	bucket []byte
}

func (e *Engine) Get(k string) ([]byte, error) {
	var v []byte

	err := e.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(e.bucket)
		v = b.Get([]byte(k))
		return nil
	})

	if err != nil {
		return nil, err
	}

	return v, nil
}

func (e *Engine) Set(k string, v []byte) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(e.bucket)
		return b.Put([]byte(k), v)
	})
}

func (e *Engine) SetMany(b storage.Batch) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		var (
			c   = tx.Bucket(e.bucket)
			err error
		)

		for k, v := range b {
			err = c.Put([]byte(k), v)

			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func Open(opts *storage.Options) (*Engine, error) {
	if opts.Path == "" {
		return nil, ErrPathRequired
	}

	db, err := bolt.Open(opts.Path, 0600, nil)

	if err != nil {
		return nil, err
	}

	e := Engine{
		db:     db,
		bucket: []byte("origins"),
	}

	// TODO(bjr): split keys in different buckets per domain?
	err = e.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(e.bucket)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &e, nil
}
