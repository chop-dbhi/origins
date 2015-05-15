package origins

import "io"

// Reader is an interface that defines the Read method. It takes a buffer
// of facts and returns the number of facts read and an error. When the
// reader is exhausted the number of facts read will be zero.
type Reader interface {
	Read(Facts) (int, error)
}

// Iterator is an interface that defines the Next method. When the iterator
// is exhausted, the fact will be nil.
type Iterator interface {
	Next() (*Fact, error)
}

// factReader wraps an existing set of facts to be exposed as a fact.Reader.
type FactReader struct {
	// Filter is a filter function
	Filter Filter

	facts Facts

	// Current position in the facts.
	pos int
}

func (r *FactReader) Read(buf Facts) (int, error) {
	size := len(r.facts)

	// Consumed.
	if r.pos >= size {
		return 0, io.EOF
	}

	var err error

	blen := len(buf)

	// No filter, do more efficient copy.
	if r.Filter == nil {
		lo := r.pos
		hi := lo + blen

		// Cap hi to the size of the facts.
		if hi >= size {
			hi = size
			err = io.EOF
		}

		// Actual number observed.
		n := hi - lo

		if n > 0 {
			copy(buf, r.facts[lo:hi])
			r.pos += n
		}

		return n, err
	}

	// Consume the facts slice until the buffer is filled or the
	// slice runs out.

	var (
		n, j int
		f    *Fact
	)

	// Start at the last position.
	for _, f = range r.facts[r.pos:] {
		if j == blen {
			break
		}

		n += 1

		// Add to buffer if passes.
		if r.Filter(f) {
			buf[j] = f
			j += 1
		}
	}

	// Update the offset with the
	r.pos += n

	if r.pos >= size {
		err = io.EOF
	}

	return j, err
}

// NewReader returns a reader that wraps an existing slice of facts to expose
// a fact.Reader interface.
func NewReader(facts Facts) *FactReader {
	return &FactReader{
		facts: facts,
	}
}

// ReadAll is reads all statements from a Reader.
func ReadAll(r Reader) (Facts, error) {
	var (
		n, i int
		err  error
	)

	buf := make(Facts, 100)
	out := make(Facts, 0)

	for {
		n, err = r.Read(buf)

		// Append facts from buffer.
		if n > 0 {
			i += n
			out = append(out, buf[:n]...)
		}

		// Reader is consumed.
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}
	}

	return out[:i], nil
}

// multiReader chains together multiple readers. This makes it seamless to
// consume facts from multiple physical locations.
type multiReader struct {
	r       Reader
	readers []Reader
	index   int
}

func (mr *multiReader) Read(buf Facts) (int, error) {
	if mr.index >= len(mr.readers) {
		return 0, io.EOF
	}

	if mr.r == nil {
		mr.r = mr.readers[mr.index]
	}

	var (
		n, m int
		err  error
	)

	for {
		// Read facts into the buffer.
		n, err = mr.r.Read(buf[n:])

		// Current reader is consumed. Go to the next one or return,
		// otherwise break the loop.
		if err == io.EOF {
			mr.index += 1

			// Break to exit.
			if mr.index >= len(mr.readers) {
				break
			}

			// Set the new reader
			mr.r = mr.readers[mr.index]

			// Increment total count of facts read across readers.
			m += n
		} else {
			break
		}
	}

	return n + m, err
}

// MultiReader takes zero or more readers and returns a single reader
// that will consume the readers in order.
func MultiReader(readers ...Reader) Reader {
	return &multiReader{
		readers: readers,
	}
}
