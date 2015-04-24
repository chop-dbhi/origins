package fact

import "io"

// Writer is an interface that define the Write method. It takes a slice of
// facts and writes them to the underlying stream. It returns the number of facts
// written and an error.
type Writer interface {
	Write(Facts) (int, error)
}

// ReadWriter reads all facts from the reader and writes them to the writer.
// The number of facts written is returned and an error if present.
func ReadWriter(reader Reader, writer Writer) (int, error) {
	var (
		n, rn, wn  int
		rerr, werr error
	)

	buf := make(Facts, 100)

	for {
		rn, rerr = reader.Read(buf)

		if rn > 0 {
			wn, werr = writer.Write(buf[:rn])

			n += wn

			// Error writing
			if werr != nil {
				return n, werr
			}
		}

		if rerr == io.EOF {
			break
		}

		if rerr != nil {
			return n, rerr
		}
	}

	return n, nil
}
