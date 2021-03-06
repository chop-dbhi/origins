package view

import (
	"errors"
	"io"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/dal"
	"github.com/chop-dbhi/origins/storage"
	"github.com/satori/go.uuid"
)

var ErrDoesNotExist = errors.New("log: does not exist")

// logView maintains state of a log that is being read.
type logView struct {
	name   string
	domain string
	head   *uuid.UUID

	asof  time.Time
	since time.Time

	engine  storage.Engine
	segment *dal.Segment
	err     error

	block  *dal.BlockDecoder
	bindex int
	bcount int
}

// nextSegment
func (li *logView) nextSegment() error {

	var (
		id  *uuid.UUID
		seg *dal.Segment
		err error
	)

	// Reset segment block state.
	li.bindex = 0
	li.block = nil

	// Loop until a valid segment is found.
	for {
		if li.segment == nil {
			id = li.head
		} else {
			id = li.segment.Next
		}

		// No more segments.
		if id == nil {
			return io.EOF
		}

		// Decode segment.
		if seg, err = dal.GetSegment(li.engine, li.domain, id); err != nil {
			return err
		}

		// Segment next segment. This is done before checking if it is in range so
		// it can be evaluated in the next iteration is necessary.
		li.segment = seg

		// Too late, skip segment.
		if !li.asof.IsZero() && seg.Time.After(li.asof) {
			continue
		}

		// Too early, skip segment.
		if !li.since.IsZero() && seg.Time.Before(li.since) {
			continue
		}

		break
	}

	return nil
}

// nextBlock returns the block that has the next fact or nil or no
// more blocks exist.
func (li *logView) nextBlock() error {
	// Existing block and is not empty.
	if li.block != nil && !li.block.Empty() {
		return nil
	}

	// First segment or there are no blocks left in segment.
	if li.segment == nil || li.bindex == li.segment.Blocks {
		if err := li.nextSegment(); err != nil {
			return err
		}
	}

	// Get the block.
	block, err := dal.GetBlock(li.engine, li.segment.Domain, li.segment.UUID, li.bindex)

	if err != nil {
		return err
	}

	// Block does not exist.
	if block == nil {
		return io.EOF
	}

	li.block = dal.NewBlockDecoder(block, li.segment.Domain, li.segment.Transaction)
	li.bindex++
	li.bcount = 0

	return nil
}

func (li *logView) Next() *origins.Fact {
	if li.err != nil {
		return nil
	}

	if err := li.nextBlock(); err != nil {
		li.err = err
		return nil
	}

	fact := li.block.Next()

	if fact != nil {
		li.bcount++
	}

	return fact
}

func (li *logView) Err() error {
	// No local error, defer to block decoder.
	if li.err == nil && li.block != nil {
		return li.block.Err()
	}

	if li.err == io.EOF {
		return nil
	}

	return li.err
}

// merger maintains state of a merged stream of facts from multiple domains.
type merger struct {
	iterators []origins.Iterator
	next      []*origins.Fact
	err       error
}

func (m *merger) Next() *origins.Fact {
	// Error occurred.
	if m.err != nil {
		return nil
	}

	var (
		f *origins.Fact

		// Transaction id.
		tx    uint64 = 1<<64 - 1
		maxTx uint64 = 1<<64 - 1

		// Index of iterator whose fact will be returned.
		idx int = -1
	)

	// look at the next fact coming out of each of the the available logs,
	// and choose the fact with the greatest transaction ID.
	for i, iter := range m.iterators {
		f = m.next[i]

		// No existing fact. Either the iterator is consumed, or no facts have
		// been fetched from it yet, or its fact was returned in the last
		// iteration.
		if f == nil {
			if f = iter.Next(); f == nil {
				if m.err = iter.Err(); m.err != nil {
					return nil
				}

				// This iterator is consumed. Continue to next iterator.
				continue
			}

			m.next[i] = f
		}

		// Find the highest tx ID, which denotes the newest fact.
		// (Iterators return facts in the order of newest facts first).
		if tx == maxTx || f.Transaction > tx {
			tx = f.Transaction
			idx = i
		}
	}

	// Iterators are consumed.
	if idx < 0 {
		return nil
	}

	f = m.next[idx]
	m.next[idx] = nil
	return f
}

func (m *merger) Err() error {
	return m.err
}

// Merge multiple fact streams into one.
// Note: iterators return facts in the order opposite to that in which
// they were committed (i.e. newest facts with largest transaction IDs first),
// and the Merge operation will preserve that.
func Merge(iterators ...origins.Iterator) origins.Iterator {
	return &merger{
		iterators: iterators,
		next:      make([]*origins.Fact, len(iterators)),
	}
}

// deduplicator maintains the state of a steam of unique facts
type deduplicator struct {
	iter          origins.Iterator
	observedFacts *factMap
}

func (d *deduplicator) Err() error {
	return d.iter.Err()
}

func (d *deduplicator) Next() *origins.Fact {
	if d.iter.Err() != nil {
		return nil
	}

	var f *origins.Fact

	for {
		if f = d.iter.Next(); f == nil {
			break
		}

		if !d.observedFacts.add(f) {
			return f
		}
	}

	return nil
}

// remove duplicate facts from a stream
func Deduplicate(iter origins.Iterator) origins.Iterator {
	return &deduplicator{
		iter:          iter,
		observedFacts: newFactMap(),
	}
}

// hash map for facts.
type factMap struct {
	facts map[[3]origins.Ident]*origins.Fact
}

func newFactMap() *factMap {
	return &factMap{
		facts: make(map[[3]origins.Ident]*origins.Fact),
	}
}

// add a new fact to the fact hash map;
// return true if the fact was there to begin with, and false otherwise.
func (fm *factMap) add(fact *origins.Fact) bool {
	key := [3]origins.Ident{
		*fact.Entity,
		*fact.Attribute,
		*fact.Value,
	}

	if _, exists := fm.facts[key]; exists {
		return true
	}

	fm.facts[key] = fact

	return false
}

// A Log is an ordered sequence of facts within a domain.
type Log struct {
	log *dal.Log

	engine storage.Engine
}

// View returns a view of the log for the specified time period. It is safe for
// multiple consumers to create views of a log.
func (l *Log) View(since, asof time.Time) origins.Iterator {
	return &logView{
		domain: l.log.Domain,
		head:   l.log.Head,
		engine: l.engine,
		since:  since,
		asof:   asof,
	}
}

// Now returns a view of the log with a time boundary set to the current time.
// This is equivalent to: Asof(time.Now().UTC())
func (l *Log) Now() origins.Iterator {
	return l.Asof(time.Now())
}

// Asof returns a view of the log with an explicit upper time boundary.
func (l *Log) Asof(t time.Time) origins.Iterator {
	return l.View(time.Time{}, t.UTC())
}

// Since returns a view of the log with an explicit lower time boundary.
func (l *Log) Since(t time.Time) origins.Iterator {
	return l.View(t.UTC(), time.Time{})
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
