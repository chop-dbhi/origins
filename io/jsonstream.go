package io

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/sirupsen/logrus"
)

// Auxillary struct to decode a JSON-encoded fact in for parsing.
type auxFact struct {
	Domain    string
	Operation string
	Time      interface{}
	Entity    interface{}
	Attribute interface{}
	Value     interface{}
}

type JSONStreamReader struct {
	reader io.Reader
	buf    []byte
	eof    bool
	err    error
}

// TODO(bjr): there is a io.ByteReader interface, however it is
// not as general as a general io.Reader.
func (r *JSONStreamReader) next() ([]byte, error) {
	if r.eof {
		return nil, nil
	}

	var (
		n   int
		pos int
		err error
	)

	buflen := len(r.buf)
	b := make([]byte, 1, 1)

	for {
		n, err = r.reader.Read(b)

		// No more bytes available, end of stream.
		if n == 0 || err == io.EOF {
			r.eof = true
			return r.buf[:pos], nil
		}

		if err != nil {
			return nil, err
		}

		if b[0] == '\n' || b[0] == '\r' {
			// There may be multiple newlines or carriage returns in
			// succession, ignore them all.
			if pos != 0 {
				return r.buf[:pos], err
			}
		} else {
			// Double the buffer
			if pos >= buflen {
				buflen = buflen * 2
				nbuf := make([]byte, buflen)

				copy(nbuf, r.buf)

				r.buf = nbuf
			}

			r.buf[pos] = b[0]

			pos += 1
		}
	}
}

// Parse JSON-encoded bytes into a fact.
func (r *JSONStreamReader) parse(b []byte) (*origins.Fact, error) {
	// Decode into an auxillary fact.
	var (
		af  auxFact
		err error
	)

	err = json.Unmarshal(b, &af)

	if err != nil {
		return nil, err
	}

	f := origins.Fact{}

	// Domain; just a string
	f.Domain = af.Domain

	// Operation
	op, err := origins.ParseOperation(af.Operation)

	if err != nil {
		logrus.Error(err)
		return nil, err
	}

	f.Operation = op

	if af.Time != nil {
		// Time; string or int
		switch x := af.Time.(type) {
		case string:
			t, err := chrono.Parse(x)

			if err != nil {
				logrus.Error(err)
				return nil, err
			}

			f.Time = t
		case int64:
			f.Time = chrono.MicroTime(x)
		default:
			err = errors.New(fmt.Sprintf("invalid time value: %v", af.Time))
			return nil, err
		}
	}

	// Entity; string (name) or object
	var (
		v            interface{}
		ok           bool
		domain, name string
		ident        *origins.Ident
	)

	switch x := af.Entity.(type) {
	case map[string]interface{}:
		if v, ok = x["domain"]; ok {
			domain = v.(string)
		}

		if v, ok = x["name"]; ok {
			name = v.(string)
		}
	case string:
		name = x
	}

	if name == "" {
		err = errors.New(fmt.Sprintf("json: invalid entity format %v", af.Entity))
		return nil, err
	}

	if ident, err = origins.NewIdent(domain, name); err != nil {
		return nil, err
	}

	f.Entity = ident

	// Reset
	domain = ""
	name = ""

	switch x := af.Attribute.(type) {
	case map[string]interface{}:
		if v, ok = x["domain"]; ok {
			domain = v.(string)
		}

		if v, ok = x["name"]; ok {
			name = v.(string)
		}
	case string:
		name = x
	}

	if name == "" {
		err = errors.New(fmt.Sprintf("json: invalid attribute format %v", af.Attribute))
		return nil, err
	}

	if ident, err = origins.NewIdent(domain, name); err != nil {
		return nil, err
	}

	f.Attribute = ident

	// Reset
	domain = ""
	name = ""

	switch x := af.Value.(type) {
	case map[string]interface{}:
		if v, ok = x["domain"]; ok {
			domain = v.(string)
		}

		if v, ok = x["name"]; ok {
			name = v.(string)
		}
	case string:
		name = x
	}

	if ident, err = origins.NewIdent(domain, name); err != nil {
		return nil, err
	}

	f.Value = ident

	// Error will be nil or EOF
	return &f, nil
}

// Next returns
func (r *JSONStreamReader) Next() *origins.Fact {
	if r.err != nil {
		return nil
	}

	b, err := r.next()

	// Real error.
	if err != nil {
		r.err = err
		return nil
	}

	f, err := r.parse(b)

	if err != nil {
		r.err = err
		return nil
	}

	return f
}

func (r *JSONStreamReader) Err() error {
	return r.err
}

// JSONStreamReader returns a reader that parsed a stream of newline-delimited
// JSON-encoded facts.
func NewJSONStreamReader(reader io.Reader) origins.Iterator {
	r := JSONStreamReader{
		reader: reader,
		buf:    make([]byte, 200),
	}

	return &r
}

type JSONStreamWriter struct {
	writer io.Writer
}

func (w *JSONStreamWriter) marshal(f *origins.Fact) []byte {
	b, err := json.Marshal(f)

	if err != nil {
		panic(err)
	}

	return b
}

func (w *JSONStreamWriter) Write(fact *origins.Fact) error {
	// Buffer to reduce the number of calls to flush.
	var (
		b   []byte
		err error
		nl  = []byte{'\n'}
	)

	b = w.marshal(fact)

	if _, err = w.writer.Write(b); err != nil {
		return err
	}

	if _, err = w.writer.Write(nl); err != nil {
		return err
	}

	return nil
}

func NewJSONStreamWriter(writer io.Writer) *JSONStreamWriter {
	return &JSONStreamWriter{
		writer: writer,
	}
}
