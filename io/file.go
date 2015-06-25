package io

import (
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"regexp"
)

var (
	csvExt  = regexp.MustCompile(`(?i)\.csv\b`)
	jsonExt = regexp.MustCompile(`(?i)\.jsons(tream)?\b`)

	gzipExt  = regexp.MustCompile(`(?i)\.(gz|gzip)\b`)
	bzip2Ext = regexp.MustCompile(`(?i)\.(bz2|bzip2)\b`)
)

// DetectFileFormat detects the file format based on the filename.
func DetectFileFormat(n string) string {
	if csvExt.MatchString(n) {
		return "csv"
	} else if jsonExt.MatchString(n) {
		return "jsonstream"
	}

	return ""
}

// DetectFileCompression detects the file compression type based on the filename.
func DetectFileCompression(n string) string {
	if gzipExt.MatchString(n) {
		return "gzip"
	} else if bzip2Ext.MatchString(n) {
		return "bzip2"
	}

	return ""
}

// Decompressor returns a reader that wraps the input reader for decompression.
func Decompressor(r io.Reader, f string) (io.Reader, error) {
	switch f {
	case "bzip2":
		return bzip2.NewReader(r), nil
	case "gzip":
		return gzip.NewReader(r)
	}

	return nil, fmt.Errorf("file: unsupported compression format %s", f)
}

// UniversalReader wraps an io.Reader and replaces carriage returns with newlines.
type UniversalReader struct {
	r io.Reader
}

func (c *UniversalReader) Read(buf []byte) (int, error) {
	n, err := c.r.Read(buf)

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}

// NewUniversalReader returns a UniversalReader that wraps the passed io.Reader.
func NewUniversalReader(r io.Reader) *UniversalReader {
	return &UniversalReader{r}
}
