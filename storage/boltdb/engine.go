/*
The disk package wraps the Diskv (https://github.com/peterbourgon/diskv) file-based
key/value store.
*/
package boltdb

import (
	"encoding/binary"
	"errors"

	"github.com/boltdb/bolt"
	"github.com/chop-dbhi/origins/storage"
)

const defaultBucketName = "origins"

var (
	ErrPathRequired = errors.New("Path to the boltdb file required")
)

type Engine struct {
	Path   string
	bucket []byte
}

func (e *Engine) Get(k string) ([]byte, error) {
	db, err := bolt.Open(e.Path, 0600, nil)

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

func (e *Engine) Incr(k string) (uint64, error) {
	db, err := bolt.Open(e.Path, 0600, nil)

	if err != nil {
		return 0, err
	}

	defer db.Close()

	var (
		id   uint64
		buf  []byte
		erri int
	)

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(e.bucket)

		key := []byte(k)

		buf = b.Get(key)

		if buf != nil {
			id, erri = binary.Uvarint(buf)

			if erri == 0 {
				return errors.New("boltdb: buffer too small for value")
			} else if erri < 0 {
				return errors.New("boltdb: value larger than 64 bits")
			}
		}

		buf = make([]byte, 8)

		id += 1

		n := binary.PutUvarint(buf, id)

		return b.Put(key, buf[:n])
	})

	if err != nil {
		return 0, err
	}

	return id, nil
}

func (e *Engine) Set(k string, v []byte) error {
	db, err := bolt.Open(e.Path, 0600, nil)

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
	db, err := bolt.Open(e.Path, 0600, nil)

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
		Path:   opts.Path,
		bucket: bucket,
	}

	return &e, nil
}
