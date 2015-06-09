package view

import (
	"errors"
	"io"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/dal"
	"github.com/satori/go.uuid"
)

var ErrDoesNotExist = errors.New("log: does not exist")

// logIter maintains state of a log that is being read.
type logIter struct {
	name   string
	domain string
	head   *uuid.UUID

	asof  time.Time
	since time.Time

	engine  storage.Engine
	segment *dal.Segment

	block  origins.Facts
	bindex int
	bpos   int
}

// nextSegment
func (li *logIter) nextSegment() error {

	var (
		id  *uuid.UUID
		seg *dal.Segment
		err error
	)

	// Reset segment block state.
	li.bindex = 0
	li.block = nil
	li.bpos = 0

	// Loop until a valid segment is found.
	for {
		if li.segment == nil {
			id = li.head
		} else {
			id = li.segment.Next
		}

		if id == nil {
			return io.EOF
		}

		seg, err = dal.GetSegment(li.engine, li.domain, id)

		if err != nil {
			return err
		}

		// Update state.
		li.segment = seg

		// Too late
		if !li.asof.IsZero() && seg.Time.After(li.asof) {
			continue
		}

		// Too early
		if !li.since.IsZero() && seg.Time.Before(li.since) {
			continue
		}

		// Segment is within range.
		break
	}

	return nil
}

// nextBlock returns the block that has the next fact or nil or no
// more blocks exist.
func (li *logIter) nextBlock() error {
	// Existing block and still has facts.
	if li.block != nil && li.bpos < len(li.block) {
		return nil
	}

	// First segment or no blocks left in segment.
	if li.segment == nil || li.bindex == li.segment.Blocks {
		if err := li.nextSegment(); err != nil {
			return err
		}
	}

	block, err := dal.GetBlock(li.engine, li.segment.Domain, li.segment.UUID, li.bindex, li.segment.Transaction)

	if err != nil {
		return err
	}

	// Block does not exist.
	if block == nil {
		return io.EOF
	}

	li.block = block
	li.bpos = 0
	li.bindex++

	return nil
}

func (li *logIter) Next() (*origins.Fact, error) {
	if err := li.nextBlock(); err != nil {
		return nil, err
	}

	fact := li.block[li.bpos]
	li.bpos++

	return fact, nil
}

// Read satisfies the Reader interface.
func (li *logIter) Read(facts origins.Facts) (int, error) {
	var (
		f   *origins.Fact
		err error
		l   = len(facts)
	)

	for i := 0; i < l; i++ {
		f, err = li.Next()

		// EOF or error
		if err != nil {
			return i, err
		}

		// Add fact.
		facts[i] = f
	}

	return l, nil
}

// A Log is an ordered sequence of facts within a domain.
type Log struct {
	log *dal.Log

	engine storage.Engine
}

// Iter returns a value that implements the Iterator interface. This can be
// called multiple times for independent consumers.
func (l *Log) Iter(since, asof time.Time) *logIter {
	return &logIter{
		domain: l.log.Domain,
		head:   l.log.Head,
		engine: l.engine,
		since:  since,
		asof:   asof,
	}
}

// Now returns a view of the log with a time boundary set to the current time.
// This is equivalent to: Asof(time.Now().UTC())
func (l *Log) Now() *logIter {
	return l.Asof(time.Now())
}

// Asof returns a view of the log with an explicit upper time boundary.
func (l *Log) Asof(t time.Time) *logIter {
	return l.Iter(time.Time{}, t.UTC())
}

// Since returns a view of the log with an explicit lower time boundary.
func (l *Log) Since(t time.Time) *logIter {
	return l.Iter(t.UTC(), time.Time{})
}

// OpenLog opens a log for reading.
func OpenLog(engine storage.Engine, domain, name string) (*Log, error) {
	log, err := dal.GetLog(engine, domain, name)

	if err != nil {
		return nil, err
	}

	if log == nil {
		return nil, ErrDoesNotExist
	}

	l := Log{
		log:    log,
		engine: engine,
	}

	return &l, nil
}
