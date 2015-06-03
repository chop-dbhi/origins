package origins

import "io"

// Writer is an interface that defines the Write method. It takes a fact
// and writes it to the underlying value.
type Writer interface {
	Write(*Fact) error
	Flush() error
}

// ReadWriter reads all facts from the reader and writes them to the writer.
// The number of facts written is returned and an error if present.
func ReadWriter(r Reader, w Writer) (int, error) {
	var (
		n, rn      int
		rerr, werr error
		f          *Fact
	)

	buf := make(Facts, 100)

	for {
		rn, rerr = r.Read(buf)

		for _, f = range buf[:rn] {
			// Error writing
			if werr = w.Write(f); werr != nil {
				return n, werr
			}
		}

		n += rn

		if rerr == io.EOF {
			break
		}

		if rerr != nil {
			return n, rerr
		}
	}

	w.Flush()

	return n, nil
}
