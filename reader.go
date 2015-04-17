package origins

import "io"

// UniversalReader wraps an io.Reader and replaces carriage returns with newlines.
type UniversalReader struct {
	Reader io.Reader
}

func (c *UniversalReader) Read(buf []byte) (int, error) {
	n, err := c.Reader.Read(buf)

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}
