package fact

import "io"

// Reader is an interface that implements methods for reading raw facts
// from a source.
type Reader interface {
	Read() (*Fact, error)
}

// ReadAll is reads all statements from a Reader.
func ReadAll(r Reader) (Facts, error) {
	var (
		i   = 0
		l   = 10
		f   *Fact
		err error
	)

	// Pre-allocate; double on next append
	facts := make(Facts, l)

	for {
		f, err = r.Read()

		// Reader is consumed.
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		// Extend the slice, double size
		if i >= l {
			l *= 2
			tmp := make(Facts, l)
			copy(tmp, facts)
			facts = tmp
		}

		facts[i] = f
		i += 1
	}

	return facts[:i], nil
}
