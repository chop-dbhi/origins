package sqlite

import (
	"database/sql"
	"errors"

	"github.com/chop-dbhi/origins/storage"
	_ "github.com/mattn/go-sqlite3"
)

var (
	ErrPathRequired = errors.New("Path to a SQLite file is required")
)

const (
	createTableStmt = `
		CREATE TABLE IF NOT EXISTS origins_store (
			key TEXT PRIMARY KEY,
			value BLOB
		)
	`

	getStmt = `
		SELECT value FROM origins_store WHERE key = ? LIMIT 1
	`

	replaceStmt = `
		INSERT OR REPLACE INTO origins_store (key, value) VALUES (?, ?)
	`
)

type Engine struct {
	Name string
	db   *sql.DB
}

func (e *Engine) Get(k string) ([]byte, error) {
	/*
		db, err := sql.Open("sqlite3", e.Name)

		// Problem opening the database.
		if err != nil {
			return nil, err
		}

		defer db.Close()
	*/
	db := e.db

	var val []byte

	// Get the value for the provided key.
	err := db.QueryRow(getStmt, k).Scan(&val)

	// Check the kind of error
	switch {
	// No row exists, just return nil.
	case err == sql.ErrNoRows:
		return nil, nil
	// Other kind of error, return it
	case err != nil:
		return nil, err
	}

	return val, nil
}

func (e *Engine) Set(k string, v []byte) error {
	/*
		db, err := sql.Open("sqlite3", e.Name)

		if err != nil {
			return err
		}

		defer db.Close()
	*/
	db := e.db

	_, err := db.Exec(replaceStmt, k, v)

	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) SetMany(b storage.Batch) error {
	tx, err := e.db.Begin()

	if err != nil {
		return err
	}

	// Prepare a statement for the batch.
	stmt, err := tx.Prepare(replaceStmt)

	if err != nil {
		tx.Rollback()
		return err
	}

	for k, v := range b {
		_, err = stmt.Exec(k, v)

		if err != nil {
			tx.Rollback()
			return nil
		}
	}

	stmt.Close()
	tx.Commit()

	return nil
}

func (e *Engine) Close() error {
	return nil
}

func Open(opts *storage.Options) (*Engine, error) {
	if opts == nil || opts.Path == "" {
		return nil, ErrPathRequired
	}

	db, err := sql.Open("sqlite3", opts.Path)

	if err != nil {
		return nil, err
	}

	// Create the default table.
	_, err = db.Exec(createTableStmt)

	if err != nil {
		return nil, err
	}

	// TODO(bjr): database is kept open, thus locking it from other usage.
	e := Engine{
		Name: opts.Path,
		db:   db,
	}

	return &e, nil
}
