/*
The disk package wraps the Diskv (https://github.com/peterbourgon/diskv) file-based
key/value store.
*/
package disk

import (
	"errors"
	"os"

	"github.com/chop-dbhi/origins/storage"
	"github.com/peterbourgon/diskv"
)

var (
	ErrPathRequired = errors.New("path to the diskv directory is required")
)

type Engine struct {
	diskv *diskv.Diskv
}

func (e *Engine) Get(k string) ([]byte, error) {
	b, err := e.diskv.Read(k)

	// Diskv returns a path error if the file does exist rather. We
	// want to catch that and return nil bytes.
	switch err.(type) {
	case *os.PathError:
		return nil, nil
	}

	return b, err
}

func (e *Engine) Set(k string, v []byte) error {
	return e.diskv.Write(k, v)
}

func (e *Engine) SetMany(b storage.Batch) error {
	var err error

	for k, v := range b {
		err = e.Set(k, v)

		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) Close() error {
	return nil
}

func Open(opts *storage.Options) (*Engine, error) {
	if opts.Path == "" {
		return nil, ErrPathRequired
	}

	diskv := diskv.New(diskv.Options{
		BasePath: opts.Path,
	})

	e := Engine{
		diskv: diskv,
	}

	return &e, nil
}
