package dal

import (
	"time"

	"github.com/satori/go.uuid"
)

// A Log represents a chain of segments with the log maintaining a pointer to
// the most recent segment in the chain.
type Log struct {
	// Name of the log. Currently, only the "commit" log is in use.
	Name string

	// Domain the log is created in.
	Domain string

	// ID of the segment that is the head of the log.
	Head *uuid.UUID
}

// Segment represents a transacted set of facts. Segments are broken up into
// fixed-sized blocks to facilitate flushing the data to disk during a
// long-running transaction. Each segment maintains the basis
type Segment struct {
	// Unique identifier of the segment.
	UUID *uuid.UUID

	// ID of the transaction this segment was created in.
	Transaction uint64

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

	// ID of the segment that acted as the basis for this one. This
	// is defined as the time the transaction starts.
	Base *uuid.UUID

	// ID of the segment that this segment succeeds. Typically this is
	// the same value as Base, except when a conflict is resolved and
	// the segment position is changed.
	Next *uuid.UUID
}
