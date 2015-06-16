package transactor

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/sirupsen/logrus"
)

const (
	macrosDomain = "origins.macros"
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
func txid(engine storage.Engine) (uint64, error) {
	return engine.Incr("origins", "tx")
}

// Options are used to supply default values as well as alter the behavior
// of a running transaction.
type Options struct {
	// Default domain for unbounded facts. If this is ommitted and a fact
	// does not specify a domain, an error will occur.
	DefaultDomain string

	// Duration of time wait to receive facts before timing out the transaction.
	ReceiveWait time.Duration

	// The router to use for the transaction.
	Router Router

	// Defines the buffer size of the channel that receives facts for processing.
	// Increasing this may increase throughput at the expense of memory.
	BufferSize int

	// If true, a zeroed fact time will be set to the transaction start time. This
	// is useful for facts that are considered "new in the world".
	SetDefaultTime bool
}

// DefaultOptions hold the default options for a transaction.
var DefaultOptions = Options{
	ReceiveWait: time.Minute,
	BufferSize:  1000,
}

// Transaction is the entrypoint for transacting facts.
type Transaction struct {
	// Unique ID for the transaction.
	ID uint64

	// The start and end time of transaction.
	StartTime time.Time
	EndTime   time.Time

	// The error that caused the transaction to fail is one occurs.
	Error error

	Engine storage.Engine

	options Options

	// Router for the transaction.
	router Router

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

	// Pipelines and channels.
	pipes map[Pipeline]chan<- *origins.Fact
}

// Info holds information about a transaction.
type Info struct {
	ID        uint64
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration
	Pipelines int
	Domains   []string
	Bytes     int
	Count     int
}

// Stats returns the stats of the transaction which aggregates
// them from the pipelines.
func (tx *Transaction) Info() *Info {
	var (
		domains      []string
		bytes, count int

		stats *Stats
	)

	for pipe := range tx.pipes {
		stats = pipe.Stats()
		domains = append(domains, stats.Domains...)
		bytes += stats.Bytes
		count += stats.Count
	}

	return &Info{
		ID:        tx.ID,
		StartTime: tx.StartTime,
		EndTime:   tx.EndTime,
		Duration:  tx.EndTime.Sub(tx.StartTime),
		Pipelines: len(tx.pipes),
		Domains:   domains,
		Bytes:     bytes,
		Count:     count,
	}
}

// evaluate evaluates a fact against the log.
func (tx *Transaction) evaluate(f *origins.Fact) error {
	if f.Domain == "" {
		if tx.options.DefaultDomain == "" {
			return ErrNoDomain
		}

		f.Domain = tx.options.DefaultDomain
	}

	// Default to assertion.
	if f.Operation == origins.Noop {
		f.Operation = origins.Assertion
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
	// Macro domain. Fact is about the domain itself.
	if fact.Entity.Domain == "origins.macros" {
		switch fact.Entity.Name {
		case "domain":
			fact.Entity.Name = fact.Domain
			fact.Domain = "origins.domains"
			fact.Entity.Domain = ""

		case "tx":
			fact.Domain = fmt.Sprintf("origins.tx.%s", fact.Domain)
			fact.Entity.Domain = ""
			fact.Entity.Name = tx.StartTime.String()

		default:
			return fmt.Errorf("transactor: unknown entity macro: %s", fact.Entity.Name)
		}
	}

	if fact.Value.Domain == "origins.macros" {
		switch fact.Value.Name {
		case "now":
			fact.Value.Domain = ""
			fact.Value.Name = tx.StartTime.String()

		default:
			return fmt.Errorf("transactor: unknown value macro: %s", fact.Value.Name)
		}
	}

	return nil
}

// route uses the router to get the pipeline for the fact.
// Each pipeline
func (tx *Transaction) route(fact *origins.Fact) error {
	var (
		ok   bool
		err  error
		pipe Pipeline
		ch   chan<- *origins.Fact
	)

	// Evaluate.
	if err = tx.evaluate(fact); err != nil {
		return err
	}

	// Convert.
	if err = tx.macro(fact); err != nil {
		return err
	}

	// Route.
	if pipe, err = tx.router.Route(fact); err != nil {
		logrus.Debugf("transactor: could not route: %s", err)
		return ErrCouldNotRoute
	}

	// Get or spawn a pipeline.
	if ch, ok = tx.pipes[pipe]; !ok {
		ch = tx.spawn(pipe)
		tx.pipes[pipe] = ch
	}

	// Send fact to the pipeline.
	ch <- fact

	return nil
}

// receive is the coordinator for receiving and routing facts.
func (tx *Transaction) receive() {
	var (
		err  error
		fact *origins.Fact
	)

	logrus.Debugf("transactor(%d): begin receiving facts", tx.ID)

loop:
	for {
		select {
		// An error occurred in a pipeline.
		case err = <-tx.errch:
			logrus.Debugf("transactor(%d): %s", tx.ID, err)
			close(tx.stream)
			break loop

		// Receive facts from stream and route to pipeline.
		// If an error occurs while routing, stop processing.
		case fact = <-tx.stream:
			if fact == nil {
				logrus.Debugf("transactor(%d): end of stream", tx.ID)
				break loop
			}

			if err = tx.route(fact); err != nil {
				logrus.Debugf("transactor(%d): error routing fact", tx.ID)
				break loop
			}

		// Transaction timeout.
		case <-time.After(tx.options.ReceiveWait):
			logrus.Debugf("transactor(%d): receive timeout", tx.ID)
			err = ErrReceiveTimeout
			break loop
		}
	}

	// Set the error if one occurred.
	tx.Error = err

	// Signal the transaction is closed.
	close(tx.done)

	// Wait for the pipelines to finish there work.
	tx.pipewg.Wait()

	// Mark the end time of processing.
	tx.EndTime = time.Now().UTC()

	// Complete the transaction by committing or aborting.
	tx.complete()

	tx.mainwg.Done()
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
}

// commit commits all the pipelines in a transaction.
func (tx *Transaction) commit() error {
	return tx.Engine.Multi(func(etx storage.Tx) error {
		for pipe, _ := range tx.pipes {
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
		for pipe, _ := range tx.pipes {
			if err := pipe.Abort(etx); err != nil {
				return err
			}

			logrus.Debugf("transactor(%d): aborted pipeline %v", tx.ID, pipe)
		}

		return nil
	})
}

// spawn all values on the channel on the pipeline.
func (tx *Transaction) spawn(pipe Pipeline) chan<- *origins.Fact {
	ch := make(chan *origins.Fact)

	tx.pipes[pipe] = ch
	tx.pipewg.Add(1)

	go func() {
		defer func() {
			close(ch)
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

			case fact = <-ch:
				if err = pipe.Receive(fact); err != nil {
					tx.errch <- err
					return
				}
			}
		}
	}()

	return ch
}

func (tx *Transaction) Cancel() error {
	tx.errch <- ErrCanceled
	tx.mainwg.Wait()
	return tx.Error
}

func (tx *Transaction) Commit() error {
	close(tx.stream)
	tx.mainwg.Wait()
	return tx.Error
}

func (tx *Transaction) Write(fact *origins.Fact) error {
	tx.stream <- fact
	return nil
}

func (tx *Transaction) Flush() error {
	return nil
}

func (tx *Transaction) Consume(pub origins.Publisher) error {
	var (
		ok   bool
		err  error
		fact *origins.Fact
	)

	// Subscribe to the publisher. It takes a channel to signal when
	// this consumer is done and returns a channel that produces facts.
	ch, errch := pub.Subscribe(tx.done)

	// Consume facts until the producer is closed. This may occur upstream
	// by the producer itself or the transaction is closed prematurely.
	for {
		select {
		case err = <-errch:
			return err

		case fact, ok = <-ch:
			// Publisher closed the channel.
			if !ok {
				return nil
			}

			tx.stream <- fact
		}
	}

	return nil
}

// New initializes and returns a transaction for passed storage engine. The options
// are used to change the behavior of the transaction itself.
func New(engine storage.Engine, options Options) (*Transaction, error) {
	// Get the next transaction ID.
	id, err := txid(engine)

	if err != nil {
		logrus.Errorf("transactor: could not generate ID: %s", err)
		return nil, ErrNoID
	}

	if options.Router == nil {
		options.Router = NewDomainRouter()
	}

	if options.ReceiveWait == 0 {
		options.ReceiveWait = DefaultOptions.ReceiveWait
	}

	if options.BufferSize == 0 {
		options.BufferSize = DefaultOptions.BufferSize
	}

	tx := Transaction{
		ID:        id,
		StartTime: time.Now().UTC(),
		Engine:    engine,
		options:   options,
		pipes:     make(map[Pipeline]chan<- *origins.Fact),
		router:    options.Router,
		stream:    make(chan *origins.Fact, options.BufferSize),
		done:      make(chan struct{}),
		errch:     make(chan error),
		pipewg:    &sync.WaitGroup{},
		mainwg:    &sync.WaitGroup{},
	}

	tx.mainwg.Add(1)

	// Start the receiving goroutine.
	go tx.receive()

	return &tx, nil
}
