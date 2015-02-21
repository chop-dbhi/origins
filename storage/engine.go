package storage

// Options is a general map of storage options.
type Options struct {
	// Path is the path to the file or directory for a filesystem-based
	// storage engine.
	Path string
}

// Batch is a set of key/value pairs for batch setting.
type Batch map[string][]byte

// Engine is an interface for defining storage engines.
type Engine interface {
	// Get takes a key and returns the associated bytes.
	Get(string) ([]byte, error)

	// Set takes a key and bytes and writes it to the store.
	Set(string, []byte) error

	// SetMany takes a batch and writes it to the store. For
	// storage engines that support atomic operations, this
	// operation prevents partial updates from occurring.
	SetMany(Batch) error

	// Close performs any shutdown or clean up operation to the
	// underlying storage engine. This is most applicable to
	// file-based storage.
	Close() error
}

// testEngine satisfies the Engine interface and is for testing purposes.
type testEngine struct {
	bins map[string][]byte
}

func (e *testEngine) Get(k string) ([]byte, error) {
	return e.bins[k], nil
}

func (e *testEngine) Set(k string, v []byte) error {
	e.bins[k] = v
	return nil
}

func (e *testEngine) SetMany(b Batch) error {
	for k, v := range b {
		e.bins[k] = v
	}

	return nil
}

func (e *testEngine) Close() error {
	return nil
}

// Open initializes a new Engine and returns it.
func open(opts *Options) (*testEngine, error) {
	e := testEngine{
		bins: make(map[string][]byte),
	}

	return &e, nil
}
