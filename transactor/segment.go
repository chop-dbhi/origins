package transactor

import (
	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/dal"
	"github.com/chop-dbhi/origins/storage"
	"github.com/satori/go.uuid"
)

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
	block origins.Facts
	index int
}

// Write writes the current block to the storage and updates the Segment header.
func (s *Segment) Write(tx storage.Tx) error {
	if s.index == 0 {
		return nil
	}

	var (
		err  error
		size int
	)

	// Returns the number of bytes written and an error.
	if size, err = dal.SetBlock(tx, s.Domain, s.UUID, s.Blocks, s.block[:s.index]); err != nil {
		return err
	}

	// Update stats before set the segment.
	s.Bytes += size
	s.Count += s.index
	s.Blocks++

	// Reset block index.
	s.index = 0

	// Pass the embedded DAL segment.
	if _, err := dal.SetSegment(tx, s.Domain, &s.Segment); err != nil {
		return err
	}

	return nil
}

// Append appends a fact to the current block.
func (s *Segment) Append(f *origins.Fact) error {
	s.block[s.index] = f
	s.index += 1

	if s.index == blockSize {
		return s.Engine.Multi(func(tx storage.Tx) error {
			if err := s.Write(tx); err != nil {
				return err
			}

			return nil
		})
	}

	return nil
}

func (s *Segment) Abort(tx storage.Tx) error {
	var xrr, err error

	err = dal.DeleteSegment(tx, s.Domain, s.UUID)

	for i := 0; i < s.Blocks; i++ {
		if xrr = dal.DeleteBlock(tx, s.Domain, s.UUID, i); err != nil {
			err = xrr
		}
	}

	return err
}

func NewSegment(engine storage.Engine, domain string, tx uint64) *Segment {
	u := uuid.NewV4()

	return &Segment{
		Engine: engine,
		Segment: dal.Segment{
			UUID:        &u,
			Transaction: tx,
			Domain:      domain,
		},
		block: make(origins.Facts, blockSize),
	}
}
