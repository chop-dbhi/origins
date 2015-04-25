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

const defaultBucketName = "origins"

var (
	ErrPathRequired = errors.New("Path to the boltdb file required")
)

type Engine struct {
	Options storage.Options
	path    *bolt.DB
	bucket  []byte
}

func (e *Engine) Get(k string) ([]byte, error) {
	db, err := bolt.Open(e.Options.Path, 0600, nil)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	var s, v []byte

	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(e.bucket)
		s = b.Get([]byte(k))
		v = make([]byte, len(s))
		copy(v, s)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return v, nil
}

func (e *Engine) Set(k string, v []byte) error {
	db, err := bolt.Open(e.Options.Path, 0600, nil)

	if err != nil {
		return err
	}

	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(e.bucket)
		return b.Put([]byte(k), v)
	})
}

func (e *Engine) SetMany(b storage.Batch) error {
	db, err := bolt.Open(e.Options.Path, 0600, nil)

	if err != nil {
		return err
	}

	defer db.Close()

	return db.Update(func(tx *bolt.Tx) error {
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
	return nil
}

func Open(opts *storage.Options) (*Engine, error) {
	if opts.Path == "" {
		return nil, ErrPathRequired
	}

	db, err := bolt.Open(opts.Path, 0600, nil)

	if err != nil {
		return nil, err
	}

	defer db.Close()

	bucket := []byte(defaultBucketName)

	// Create the default bucket for the database.
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	})

	if err != nil {
		return nil, err
	}

	e := Engine{
		Options: *opts,
		bucket:  bucket,
	}

	return &e, nil
}
