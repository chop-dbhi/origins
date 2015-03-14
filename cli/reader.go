package main

import "io"

// universalReader wraps an io.Reader and replaces carriage returns with newlines.
type UniversalReader struct {
	reader io.Reader
}

func (c *UniversalReader) Read(buf []byte) (int, error) {
	n, err := c.reader.Read(buf)

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}
