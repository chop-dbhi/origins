package fact

import (
	"io"
)

// Reader is an interface that define the Read method. It takes a slice of
// facts and returns the number of facts read and an error.
type Reader interface {
	Read(Facts) (int, error)
}

// ReadAll is reads all statements from a Reader.
func ReadAll(r Reader) (Facts, error) {
	var (
		n   int
		err error
	)

	// Pre-allocate; double on next append
	facts := make(Facts, 0)
	buf := make(Facts, 100)

	for {
		n, err = r.Read(buf)

		// Append facts from buffer.
		if n > 0 {
			facts = append(facts, buf...)
		}

		// Reader is consumed.
		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}
	}

	return facts[:n], nil
}
