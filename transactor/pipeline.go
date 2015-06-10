package transactor

import (
	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/dal"
	"github.com/chop-dbhi/origins/storage"
	"github.com/satori/go.uuid"
)

const commitLogName = "commit"

// Stats contains information about a pipeline.
type Stats struct {
	// List of domains that were affected.
	Domains []string

	// Number of segments.
	Segments int

	// Total number of blocks.
	Blocks int

	// Total of number bytes.
	Bytes int

	// Total number of facts.
	Count int
}

// A Pipeline does the actual work of processing and writing facts to storage.
type Pipeline interface {
	// Init initializes the pipeline for the transaction.
	Init(*Transaction) error

	// Receive takes a fact and returns an error if the fact cannot be handled.
	Receive(*origins.Fact) error

	// Commit takes a storage transaction and writes any headers or indexes to make
	// the transacted facts visible. A storage transaction is passed in to enable
	// the writes to occur atomically which ensures consistency of the transacted
	// facts.
	Commit(storage.Tx) error

	// Abort aborts the pipeline and deletes any data written to storage.
	Abort(storage.Tx) error

	// Segments returns a slice of segments that were written to storage.
	Stats() *Stats
}

type DomainPipeline struct {
	Domain  string
	segment *Segment
}

func (p *DomainPipeline) String() string {
	return p.Domain
}

// Stats returns the the stats for the pipeline.
func (p *DomainPipeline) Stats() *Stats {
	return &Stats{
		Domains:  []string{p.segment.Domain},
		Segments: 1,
		Blocks:   p.segment.Blocks,
		Bytes:    p.segment.Bytes,
		Count:    p.segment.Count,
	}
}

func (p *DomainPipeline) Init(tx *Transaction) error {
	// Get the commit log for this domain.
	log, err := dal.GetLog(tx.Engine, p.Domain, commitLogName)

	if err != nil {
		return err
	}

	if log == nil {
		log = &dal.Log{}
	}

	// Initialize new segment pointing to the head of the log.
	p.segment = NewSegment(tx.Engine, p.Domain, tx.ID)
	p.segment.Base = log.Head
	p.segment.Next = log.Head
	p.segment.Time = tx.StartTime

	return nil
}

func (p *DomainPipeline) Receive(fact *origins.Fact) error {
	return p.segment.Append(fact)
}

func (p *DomainPipeline) Abort(tx storage.Tx) error {
	return p.segment.Abort(tx)
}

func (p *DomainPipeline) Commit(tx storage.Tx) error {
	var (
		err error
		log *dal.Log
	)

	// Write the remaining block of the segment.
	if err = p.segment.Write(tx); err != nil {
		return err
	}

	// Compare and swap ID on domain's commit log.
	if log, err = dal.GetLog(tx, p.Domain, commitLogName); err != nil {
		return err
	}

	// Existing commit log, check if the head is the same.
	if log != nil {
		// TODO: determine path to handle conflicts.
		if log.Head != nil && !uuid.Equal(*log.Head, *p.segment.Base) {
			return ErrCommitConflict
		}
	} else {
		log = &dal.Log{
			Name:   commitLogName,
			Domain: p.Domain,
		}
	}

	log.Head = p.segment.UUID

	_, err = dal.SetLog(tx, p.Domain, log)

	return err
}
