package fact

import "io"

// Reader is an interface that defines the Read method. It takes a slice of
// facts and returns the number of facts read and an error.
type Reader interface {
	Read(Facts) (int, error)
}

// Filter is function type that takes a fact and returns true if it passes
// the predicates.
type Filter func(f *Fact) bool

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

// Reads facts until the predicate matches.
func Exists(r Reader, pred Filter) (bool, error) {
	var (
		n   int
		f   *Fact
		err error
	)

	buf := make(Facts, 10)

	for {
		n, err = r.Read(buf)

		if n > 0 {
			for _, f = range buf[:n] {
				if pred(f) {
					return true, nil
				}
			}
		}

		// Reader is consumed.
		if err == io.EOF {
			break
		}

		if err != nil {
			return false, err
		}
	}

	return false, nil
}
