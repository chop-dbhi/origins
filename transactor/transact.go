package transactor

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/chop-dbhi/origins/storage"
)

var (
	ErrCanceled       = errors.New("transactor: canceled")
	ErrNoID           = errors.New("transactor: could not create tx id")
	ErrCommitConflict = errors.New("transactor: commit conflict")
	ErrReceiveTimeout = errors.New("transactor: receive timeout")
	ErrNoDomain       = errors.New("transactor: no fact domain")
	ErrCouldNotRoute  = errors.New("transactor: could not route")
)

// txid increments a global transaction ID.
func txid(tx storage.Tx) (uint64, error) {
	return tx.Incr("origins", "tx")
}

// Options are used to supply default values as well as alter the behavior
// of a running transaction.
type Options struct {
	// Default domain for unbounded facts. If this is ommitted and a fact
	// does not specify a domain, an error will occur.
	DefaultDomain string

	// Duration of time wait to receive facts before timing out the transaction.
	ReceiveWait time.Duration

	// Defines the buffer size of the channel that receives facts for processing.
	// Increasing this may increase throughput at the expense of memory.
	BufferSize int

	// If true, a zeroed fact time will be set to the transaction start time. This
	// is useful for facts that are considered "new in the world".
	SetDefaultTime bool

	// If true, duplicates will facts will be written to storage.
	AllowDuplicates bool
}

// DefaultOptions hold the default options for a transaction.
var DefaultOptions = Options{
	ReceiveWait:     time.Minute,
	BufferSize:      1000,
	AllowDuplicates: false,
}

// Transaction is the entrypoint for transacting facts.
type Transaction struct {
	// Unique ID for the transaction.
	ID uint64

	// The start and end time of transaction.
	StartTime time.Time
	EndTime   time.Time

	// Error during the transaction and commit error.
	Error       error
	CommitError error

	Engine storage.Engine

	options Options

	// Main channel received facts are received by.
	stream chan *origins.Fact

	// Channel that can be passed around for signaling the
	// transaction stream has been closed.
	done chan struct{}

	// Shared error channel for all goroutines to communicate when an
	// error occurs.
	errch chan error

	// Wait groups for main goroutine and pipelines.
	mainwg *sync.WaitGroup
	pipewg *sync.WaitGroup

	// Domain to pipeline.
	pipes map[string]*Pipeline

	// Set of domains that were transacted.
	domains []string

	// Entity ident of transaction.
	entity *origins.Ident
}

// Info holds information about a transaction.
type Info struct {
	ID        uint64
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Domains   []*Stats
	Bytes     int
	Count     int
}

// Stats returns the stats of the transaction which aggregates
// them from the pipelines.
func (tx *Transaction) Info() *Info {
	var (
		bytes int
		count int
		s     *Stats
		stats = make([]*Stats, len(tx.domains))
	)

	var i int

	for _, d := range tx.domains {
		s = tx.pipes[d].Stats()

		bytes += s.Bytes
		count += s.Count

		stats[i] = s
		i++
	}

	return &Info{
		ID:        tx.ID,
		StartTime: tx.StartTime,
		EndTime:   tx.EndTime,
		Duration:  tx.EndTime.Sub(tx.StartTime),
		Domains:   stats,
		Bytes:     bytes,
		Count:     count,
	}
}

// defaults fills in the default values for the fact.
func (tx *Transaction) defaults(f *origins.Fact) error {
	if f.Domain == "" {
		if tx.options.DefaultDomain == "" {
			return ErrNoDomain
		}

		f.Domain = tx.options.DefaultDomain
	}

	// Default to fact domain.
	if f.Entity.Domain == "" {
		f.Entity.Domain = f.Domain
	}

	if f.Attribute.Domain == "" {
		f.Attribute.Domain = f.Domain
	}

	// Set fact time to the transaction time if flagged to do so.
	if f.Time.IsZero() && tx.options.SetDefaultTime {
		f.Time = tx.StartTime
	}

	return nil
}

// macro takes a fact and resolves the macro.
func (tx *Transaction) macro(fact *origins.Fact) error {
	// Do not process facts for the macros themselves.
	if fact.Domain == origins.MacrosDomain {
		return nil
	}

	// Entity-based macro.
	if fact.Entity.Domain == origins.MacrosDomain {
		switch fact.Entity.Name {

		// Facts about the domain.
		case "domain":
			fact.Entity.Name = fact.Domain
			fact.Domain = origins.DomainsDomain
			fact.Entity.Domain = ""

		// Facts about the transaction.
		case "tx":
			fact.Domain = origins.TransactionsDomain
			fact.Entity.Domain = ""
			fact.Entity.Name = fmt.Sprint(tx.ID)

		default:
			return fmt.Errorf("transactor(%d): unknown entity macro: %s", tx.ID, fact.Entity.Name)
		}
	}

	// Value-based macro.
	if fact.Value.Domain == origins.MacrosDomain {
		switch fact.Value.Name {

		// Set the value to the transaction time.
		case "now":
			fact.Value.Domain = ""
			fact.Value.Name = tx.StartTime.String()

		// Set the value to the transaction entity.
		case "tx":
			fact.Value.Domain = origins.TransactionsDomain
			fact.Value.Name = fmt.Sprint(tx.ID)

		default:
			return fmt.Errorf("transactor(%s): unknown value macro: %s", tx.ID, fact.Value.Name)
		}
	}

	return nil
}

// route uses the router to get the pipeline for the fact.
func (tx *Transaction) route(fact *origins.Fact, track bool) error {
	var (
		ok   bool
		err  error
		pipe *Pipeline
	)

	// Defaults.
	if err = tx.defaults(fact); err != nil {
		return err
	}

	// Convert.
	if err = tx.macro(fact); err != nil {
		return err
	}

	// Initialize a pipeline for the domain if one does not exit.
	if pipe, ok = tx.pipes[fact.Domain]; !ok {
		if track {
			tx.domains = append(tx.domains, fact.Domain)
		}

		pipe = tx.spawn(fact.Domain)
		tx.pipes[fact.Domain] = pipe
	}

	// Send fact to the pipeline.
	pipe.receiver <- fact

	return nil
}

func (tx *Transaction) run() {
	// Start the receiver. This blocks until the stream ends or is interrupted.
	err := tx.receive()

	if err == nil {
		// Collect the domains affected across transacted facts and generates
		// facts about them.
		var (
			domain string
			fact   *origins.Fact
		)

		identAttr := &origins.Ident{
			Domain: origins.AttrsDomain,
			Name:   "ident",
		}

		for _, domain = range tx.domains {
			// Transact the identity attribute.
			fact = &origins.Fact{
				Domain: origins.DomainsDomain,
				Entity: &origins.Ident{
					Name: domain,
				},
				Attribute: identAttr,
				Value: &origins.Ident{
					Name: domain,
				},
			}

			if err = tx.route(fact, false); err != nil {
				break
			}

			// Transact a fact that declares the domain has been seen by this
			// transaction.
			fact = &origins.Fact{
				Domain: origins.DomainsDomain,
				Entity: &origins.Ident{
					Name: domain,
				},
				Attribute: &origins.Ident{
					Name: "ping",
				},
				Value: tx.entity,
			}

			if err = tx.route(fact, false); err != nil {
				break
			}
		}
	}

	// Set the error.
	tx.Error = err

	// Mark the end time of processing.
	tx.EndTime = time.Now().UTC()

	// Transact the end time.
	tx.route(&origins.Fact{
		Domain: origins.TransactionsDomain,
		Entity: tx.entity,
		Attribute: &origins.Ident{
			Name: "endTime",
		},
		Value: &origins.Ident{
			Name: chrono.FormatNano(tx.EndTime),
		},
	}, false)

	// Error during the transaction that caused the abort.
	if tx.Error != nil {
		tx.route(&origins.Fact{
			Domain: origins.TransactionsDomain,
			Entity: tx.entity,
			Attribute: &origins.Ident{
				Name: "error",
			},
			Value: &origins.Ident{
				Name: fmt.Sprint(tx.Error),
			},
		}, false)
	}

	// Signal the transaction is closed so no more external facts are received.
	close(tx.done)

	// Wait for the pipelines to finish there work.
	tx.pipewg.Wait()

	// Complete the transaction by committing or aborting.
	tx.complete()

	// Signal the main goroutine is done.
	tx.mainwg.Done()
}

// receive is the coordinator for receiving and routing facts.
func (tx *Transaction) receive() error {
	var (
		err  error
		fact *origins.Fact
	)

	logrus.Debugf("transactor(%d): begin receiving facts", tx.ID)

	for {
		select {
		// An error occurred in a pipeline.
		case err = <-tx.errch:
			logrus.Debugf("transactor(%d): %s", tx.ID, err)
			close(tx.stream)
			return err

		// Receive facts from stream and route to pipeline.
		// If an error occurs while routing, stop processing.
		case fact = <-tx.stream:
			if fact == nil {
				logrus.Debugf("transactor(%d): end of stream", tx.ID)
				return nil
			}

			if err = tx.route(fact, true); err != nil {
				logrus.Debugf("transactor(%d): error routing fact", tx.ID)
				return err
			}

		// Transaction timeout.
		case <-time.After(tx.options.ReceiveWait):
			logrus.Debugf("transactor(%d): receive timeout", tx.ID)
			return ErrReceiveTimeout
		}
	}

	return nil
}

// complete commits or aborts the transaction depending if an error occurred
// during processing.
func (tx *Transaction) complete() {
	var err error

	// No error in the transaction, commit the transaction.
	if tx.Error == nil {
		if err = tx.commit(); err != nil {
			logrus.Errorf("transactor(%d): commit failed: %s", tx.ID, err)
		} else {
			logrus.Debugf("transactor(%d): commit succeeded", tx.ID)
		}
	}

	// Error occurred in the transaction or during the commit. Attempt to abort.
	// TODO: if the abort fails, how can the storage garbage be reclaimed?
	if tx.Error != nil || err != nil {
		if err = tx.abort(); err != nil {
			logrus.Errorf("transactor(%d): abort failed: %s", tx.ID, err)
		} else {
			logrus.Debugf("transactor(%d): abort succeeded", tx.ID)
		}
	}

	if err != nil {
		logrus.Errorf("transactor(%d): error writing transaction record: %s", tx.ID, err)
	}

	tx.CommitError = err
}

// commit commits all the pipelines in a transaction.
func (tx *Transaction) commit() error {
	return tx.Engine.Multi(func(etx storage.Tx) error {
		for _, pipe := range tx.pipes {
			if err := pipe.Commit(etx); err != nil {
				return err
			}

			logrus.Debugf("transactor(%d): committed pipeline %v", tx.ID, pipe)
		}

		return nil
	})
}

// abort aborts all of the pipelines in a transaction.
func (tx *Transaction) abort() error {
	return tx.Engine.Multi(func(etx storage.Tx) error {
		for _, pipe := range tx.pipes {
			if err := pipe.Abort(etx); err != nil {
				return err
			}

			logrus.Debugf("transactor(%d): aborted pipeline %v", tx.ID, pipe)
		}

		return nil
	})
}

// spawn creates a pipeline for the domain and spawns a goroutine to receive facts.
func (tx *Transaction) spawn(domain string) *Pipeline {
	pipe := &Pipeline{
		Domain:   domain,
		receiver: make(chan *origins.Fact),
	}

	tx.pipewg.Add(1)

	go func() {
		defer func() {
			close(pipe.receiver)
			tx.pipewg.Done()
		}()

		var (
			err  error
			fact *origins.Fact
		)

		// Initialize the pipeline. If an error occurs, send it to the transaction's error channel
		// which will trigger the cancellation procedure.
		if err = pipe.Init(tx); err != nil {
			tx.errch <- err
			return
		}

		logrus.Debugf("transactor(%d): initialized pipeline %T(%s)", tx.ID, pipe, pipe)

		// Reads facts from the channel until there are no more or break
		// if an error occurs from the pipeline.
		for {
			select {
			case <-tx.done:
				return

			case fact = <-pipe.receiver:
				if err = pipe.Handle(fact); err != nil {
					tx.errch <- err
					return
				}
			}
		}
	}()

	return pipe
}

// Write writes a fact to the transaction.
func (tx *Transaction) Write(fact *origins.Fact) error {
	tx.stream <- fact
	return nil
}

// Cancel cancels the transaction.
func (tx *Transaction) Cancel() error {
	tx.errch <- ErrCanceled
	tx.mainwg.Wait()
	return tx.Error
}

// Commit commits the transaction. All head of all affected logs will be
// atomically updated to make the transacted data visible to clients.
func (tx *Transaction) Commit() error {
	close(tx.stream)
	tx.mainwg.Wait()
	return tx.Error
}

// New initializes and returns a transaction for passed storage engine. The options
// are used to change the behavior of the transaction itself.
func New(engine storage.Engine, options Options) (*Transaction, error) {
	var (
		id  uint64
		err error
	)

	// Start time of the transaction.
	startTime := time.Now().UTC()

	// Increment the transaction ID.
	if id, err = txid(engine); err != nil {
		logrus.Errorf("transactor: could not create transaction: %s", err)
		return nil, ErrNoID
	}

	if options.ReceiveWait == 0 {
		options.ReceiveWait = DefaultOptions.ReceiveWait
	}

	if options.BufferSize == 0 {
		options.BufferSize = DefaultOptions.BufferSize
	}

	tx := Transaction{
		ID:        id,
		StartTime: startTime,
		Engine:    engine,
		options:   options,
		pipes:     make(map[string]*Pipeline),
		stream:    make(chan *origins.Fact, options.BufferSize),
		done:      make(chan struct{}),
		errch:     make(chan error),
		pipewg:    &sync.WaitGroup{},
		mainwg:    &sync.WaitGroup{},

		entity: &origins.Ident{
			Name: fmt.Sprint(id),
		},
	}

	tx.mainwg.Add(1)

	// Run transaction in the background.
	go tx.run()

	// Write facts about the transaction including the identity
	// and start time.
	tx.Write(&origins.Fact{
		Domain: origins.TransactionsDomain,
		Entity: tx.entity,
		Attribute: &origins.Ident{
			Domain: origins.AttrsDomain,
			Name:   "ident",
		},
		Value: &origins.Ident{
			Name: tx.entity.Name,
		},
	})

	tx.Write(&origins.Fact{
		Domain: origins.TransactionsDomain,
		Entity: tx.entity,
		Attribute: &origins.Ident{
			Name: "startTime",
		},
		Value: &origins.Ident{
			Name: chrono.FormatNano(tx.StartTime),
		},
	})

	return &tx, nil
}
