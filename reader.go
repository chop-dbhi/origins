package origins

import "io"

// Reader is an interface that defines the Read method. It takes a buffer
// of facts and returns the number of facts read and an error. When the
// reader is exhausted the number of facts read will be zero.
type Reader interface {
	Read(Facts) (int, error)
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
