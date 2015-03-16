package fact

import "io"

// Reader is an interface that define the Read method. It takes a slice of
// facts and returns the number of facts read and an error.
type Reader interface {
	Read(Facts) (int, error)
}

// factReader wraps an existing set of facts to be exposed as a fact.Reader.
type factReader struct {
	facts Facts

	// Current position in the facts.
	pos int
}

func (r *factReader) Read(buf Facts) (int, error) {
	var err error

	size := len(r.facts)
	blen := len(buf)

	lo := r.pos
	hi := lo + blen

	// Cap hi to the size of the facts.
	if hi >= size {
		hi = size
		err = io.EOF
	}

	// Actual number copied.
	n := hi - lo

	if n > 0 {
		copy(buf, r.facts[lo:hi])
		r.pos += n
	}

	return n, err
}

// NewReader returns a reader that wraps an existing slice of facts to expose
// a fact.Reader interface.
func NewReader(facts Facts) *factReader {
	return &factReader{
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
			out = append(out, buf...)
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
