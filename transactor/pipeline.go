package transactor

import (
	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
)

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
}

type DomainPipeline struct {
	Domain  string
	Segment *Segment
}

func (p *DomainPipeline) Init(tx *Transaction) error {
	var (
		b   []byte
		err error
	)

	// Get the commit log for this domain.
	log := Log{}

	if b, err = tx.Storage.Get(p.Domain, LogKey); err != nil {
		return err
	}

	if err = unmarshalLog(b, &log); err != nil {
		return err
	}

	// Initialize new segment pointing to the head of the log.
	p.Segment = NewSegment(tx.Storage, tx.ID, p.Domain)
	p.Segment.Base = log.Head
	p.Segment.Next = log.Head

	return nil
}

func (p *DomainPipeline) Receive(fact *origins.Fact) error {
	return p.Segment.Append(fact)
}

func (p *DomainPipeline) Abort(tx storage.Tx) error {
	return p.Segment.Abort(tx)
}

func (p *DomainPipeline) Commit(tx storage.Tx) error {
	var (
		b   []byte
		err error
	)

	// Write the remaining block of the segment.
	if err = p.Segment.Write(tx); err != nil {
		return err
	}

	log := Log{}

	// Compare and swap ID on domain's commit log.
	if b, err = tx.Get(p.Domain, LogKey); err != nil {
		return err
	}

	// Existing commit log, check if the head is the same.
	if b != nil {
		if err = unmarshalLog(b, &log); err != nil {
			return err
		}

		// TODO: determine path to handle conflicts.
		if log.Head > 0 && log.Head != p.Segment.Base {
			return ErrCommitConflict
		}
	}

	log.Head = p.Segment.ID

	if b, err = marshalLog(&log); err != nil {
		return err
	}

	tx.Set(p.Domain, LogKey, b)

	return nil
}
