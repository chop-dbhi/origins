package transactor

import (
	"fmt"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
)

const (
	// Templates for various keys.
	LogKey     = "log.commit"
	SegmentKey = "segment.%d"
	BlockKey   = "segment.%d.%d"
)

// Number of facts written to a block.
var blockSize = 1000

// A Log represents a chain of segments with the log maintaining a pointer to
// the most recent segment in the chain.
type Log struct {
	Head uint64
}

// Segment represents a transacted set of facts. Segments are broken up into
// fixed-sized blocks to facilitate flushing the data to disk during a
// long-running transaction. Each segment maintains the basis
type Segment struct {
	// ID of the segment. This is also the ID of the transaction that
	// resulted in this segment.
	ID uint64

	// The domain this segment corresponds to.
	Domain string

	// Time the segment was committed.
	Time time.Time

	// Number of blocks in this segment.
	Blocks int

	// Total number of facts in the segment.
	Count int

	// Total number of bytes of the segment take up.
	Bytes int

	// ID of the segment that acted as the basis for this one.
	Base uint64

	// ID of the segment that follows this one. Typically this is
	// the same value as Base, except when a conflict is resolved and
	// the segment position is changed.
	Next uint64

	Storage storage.Engine

	// Block of facts.
	block origins.Facts
	index int
}

// Write writes the current block to the storage and updates the Segment header.
func (s *Segment) Write(tx storage.Tx) error {
	if s.index == 0 {
		return nil
	}

	SegmentKey := fmt.Sprintf(SegmentKey, s.ID)
	blockKey := fmt.Sprintf(BlockKey, s.ID, s.Blocks)

	var (
		err    error
		sb, bb []byte
	)

	// Marshal only up to the block index.
	if bb, err = marshalFacts(s.block[:s.index]); err != nil {
		return err
	}

	// Update stats.
	s.Bytes += len(bb)
	s.Count += s.index

	if sb, err = marshalSegment(s); err != nil {
		return err
	}

	if err = tx.Set(s.Domain, SegmentKey, sb); err != nil {
		return err
	}

	if err = tx.Set(s.Domain, blockKey, bb); err != nil {
		return err
	}

	// Reset block index and increment the number of block written
	// in the Segment.
	s.index = 0
	s.Blocks++

	return nil
}

// Append appends a fact to the current block.
func (s *Segment) Append(f *origins.Fact) error {
	s.block[s.index] = f
	s.index += 1

	if s.index == blockSize {
		return s.Storage.Multi(func(tx storage.Tx) error {
			if err := s.Write(tx); err != nil {
				return err
			}

			return nil
		})
	}

	return nil
}

func (s *Segment) Abort(tx storage.Tx) error {
	var (
		xrr, err error
		key      string
	)

	for i := 0; i < s.Blocks; i++ {
		key = fmt.Sprintf(BlockKey, s.ID, i)

		if xrr = tx.Delete(s.Domain, key); xrr != nil {
			err = xrr
		}
	}

	key = fmt.Sprintf(SegmentKey, s.ID)
	err = tx.Delete(s.Domain, key)

	return err
}

func NewSegment(s storage.Engine, id uint64, domain string) *Segment {
	return &Segment{
		ID:      id,
		Domain:  domain,
		Storage: s,
		block:   make(origins.Facts, blockSize),
	}
}
