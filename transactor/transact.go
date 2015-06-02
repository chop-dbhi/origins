package transactor

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
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
func txid(s storage.Engine) (uint64, error) {
	return s.Incr("origins", "tx")
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

	Storage storage.Engine

	options Options

	// Router for the transaction.
	router Router

	// Main channel received facts are received by.
	stream chan *origins.Fact

	// Shared error channel for all goroutines to communicate when an
	// error occurs.
	errch chan error

	// Wait groups for main goroutine and pipelines.
	mainwg *sync.WaitGroup
	pipewg *sync.WaitGroup

	// Pipelines and channels.
	pipes map[Pipeline]chan *origins.Fact
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

	// Set the valid time if empty.
	if f.Time == chrono.Zero {
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
		ch   chan *origins.Fact
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

	logrus.Debugf("transactor (%d): receiving facts", tx.ID)

loop:
	for {
		select {

		// An error occurred in a pipeline.
		case err = <-tx.errch:
			logrus.Debugf("transactor (%d): pipeline error", tx.ID)
			close(tx.stream)
			break loop

		// Receive facts from stream and route to pipeline.
		// If an error occurs while routing, stop processing.
		case fact = <-tx.stream:
			if fact == nil {
				logrus.Debugf("transactor (%d): end of stream", tx.ID)
				break loop
			}

			if err = tx.route(fact); err != nil {
				logrus.Debugf("transactor (%d): error routing fact", tx.ID)
				break loop
			}

		// Transaction timeout.
		case <-time.After(tx.options.ReceiveWait):
			logrus.Debugf("transactor (%d): timeout", tx.ID)
			err = ErrReceiveTimeout
			break loop
		}
	}

	logrus.Debugf("transactor (%d): closing pipeline channels", tx.ID)

	// Close pipes.
	for _, ch := range tx.pipes {
		close(ch)
	}

	logrus.Debugf("transactor (%d): closed pipeline channels", tx.ID)

	// Wait for the pipelines to finish there work.
	tx.pipewg.Wait()

	logrus.Debugf("transactor (%d): pipelines finished", tx.ID)

	// Set the error in case one occurred.
	tx.Error = err

	err = tx.Storage.Multi(func(etx storage.Tx) error {
		var err error

		// Close the pipeline channels.
		for pipe, _ := range tx.pipes {
			if tx.Error == nil {
				logrus.Debugf("transactor (%d): committing pipeline %v", tx.ID, pipe)

				if err = pipe.Commit(etx); err != nil {
					return err
				}
			} else {
				logrus.Debugf("transactor (%d): aborting pipeline %v", tx.ID, pipe)

				if err = pipe.Abort(etx); err != nil {
					return err
				}
			}
		}

		if err != nil {
			logrus.Debugf("transactor (%d): aborted", tx.ID)
		} else {
			logrus.Debugf("transactor (%d): committed", tx.ID)
		}

		return err
	})

	// All transaction related errors should be handled.
	if err != nil {
		panic(err)
	}

	tx.EndTime = time.Now().UTC()
	tx.mainwg.Done()
}

func (tx *Transaction) spawn(pipe Pipeline) chan *origins.Fact {
	pipech := make(chan *origins.Fact, tx.options.BufferSize)

	tx.pipes[pipe] = pipech
	tx.pipewg.Add(1)

	// Start a goroutine for this pipeline. The channel will receive
	go func() {
		defer func() {
			logrus.Debugf("transactor (%d): closing pipeline %v", tx.ID, pipe)
			tx.pipewg.Done()
		}()

		var (
			err  error
			fact *origins.Fact
		)

		// Initialize the pipeline.
		if err = pipe.Init(tx); err != nil {
			tx.errch <- err
			return
		}

		logrus.Debugf("transactor (%d): initialized pipeline %v", tx.ID, pipe)

		for {
			fact = <-pipech

			if fact == nil {
				break
			}

			// Receive takes the fact and the stream.
			if err = pipe.Receive(fact); err != nil {
				tx.errch <- err
				return
			}
		}
	}()

	return pipech
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

func (tx *Transaction) Append(fact *origins.Fact) error {
	tx.stream <- fact
	return nil
}

func (tx *Transaction) AppendIter(iter origins.Iterator) error {
	var (
		err  error
		fact *origins.Fact
	)

	for {
		fact, err = iter.Next()

		if err != nil {
			return err
		}

		if fact == nil {
			break
		}

		tx.stream <- fact
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

	tx := Transaction{
		ID:        id,
		StartTime: time.Now().UTC(),
		Storage:   engine,
		options:   options,
		pipes:     make(map[Pipeline]chan *origins.Fact),
		router:    options.Router,
		stream:    make(chan *origins.Fact, options.BufferSize),
		errch:     make(chan error),
		pipewg:    &sync.WaitGroup{},
		mainwg:    &sync.WaitGroup{},
	}

	tx.mainwg.Add(1)

	// Start the receiving goroutine.
	go tx.receive()

	return &tx, nil
}
