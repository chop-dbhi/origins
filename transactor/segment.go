package transactor

import (
	"errors"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/dal"
	"github.com/chop-dbhi/origins/storage"
	"github.com/satori/go.uuid"
)

var ErrCommitted = errors.New("transactor: segment already committed")

// Number of facts written to a block.
var blockSize = 1000

// Segment represents a transacted set of facts. Segments are broken up into
// fixed-sized blocks to facilitate flushing the data to disk during a
// long-running transaction. Each segment maintains the basis
type Segment struct {
	// Embed DAL segment for simpler access.
	dal.Segment

	Engine storage.Engine

	// Block of facts.
	block *dal.BlockEncoder

	// Flag denoting whether the segment has been committed (or aborted) in
	// which case it is an error to write more facts.
	committed bool
}

// writes the current block to the storage and updates the Segment header.
func (s *Segment) write(tx storage.Tx) error {
	if s.block.Count == 0 {
		return nil
	}

	var (
		err  error
		size int
	)

	// Returns the number of bytes written and an error.
	if size, err = dal.SetBlock(tx, s.Domain, s.UUID, s.Blocks, s.block.Bytes()); err != nil {
		return err
	}

	// Update stats before set the segment.
	s.Bytes += size
	s.Count += s.block.Count
	s.Blocks++

	// Reset block.
	s.block.Reset()

	// Pass the embedded DAL segment.
	if _, err := dal.SetSegment(tx, s.Domain, &s.Segment); err != nil {
		return err
	}

	return nil
}

// Write a fact to the current block.
func (s *Segment) Write(f *origins.Fact) error {
	if s.committed {
		return ErrCommitted
	}

	if err := s.block.Write(f); err != nil {
		return err
	}

	if s.block.Count == blockSize {
		return s.Engine.Multi(func(tx storage.Tx) error {
			if err := s.write(tx); err != nil {
				return err
			}

			return nil
		})
	}

	return nil
}

// Commit writes the last partial block.
func (s *Segment) Commit(tx storage.Tx) error {
	s.committed = true

	return s.write(tx)
}

// Abort aborts the segment and attempts to delete all data that has been
// written to storage.
func (s *Segment) Abort(tx storage.Tx) error {
	s.committed = true

	var xrr, err error

	err = dal.DeleteSegment(tx, s.Domain, s.UUID)

	for i := 0; i < s.Blocks; i++ {
		if xrr = dal.DeleteBlock(tx, s.Domain, s.UUID, i); err != nil {
			err = xrr
		}
	}

	return err
}

// NewSegment initializes a new segment for writing.
func NewSegment(engine storage.Engine, domain string, tx uint64) *Segment {
	u := uuid.NewV4()

	return &Segment{
		Engine: engine,
		block:  dal.NewBlockEncoder(),

		Segment: dal.Segment{
			UUID:        &u,
			Transaction: tx,
			Domain:      domain,
		},
	}
}
