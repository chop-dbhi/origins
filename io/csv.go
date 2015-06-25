package io

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/sirupsen/logrus"
)

var (
	ErrHeaderRequired = errors.New("A header is required with field names.")
	ErrRequiredFields = errors.New("The entity, attribute, and value fields are required.")
)

var csvHeader = []string{
	"domain",
	"operation",
	"transaction",
	"time",
	"entity_domain",
	"entity",
	"attribute_domain",
	"attribute",
	"value_domain",
	"value",
}

// parseHeader normalizes the header fields so they can be mapped to the
func parseHeader(r []string) (map[string]int, error) {
	h := make(map[string]int, len(r))

	var e, a, v bool

	for i, f := range r {
		if f == "" {
			continue
		}

		f = strings.ToLower(f)
		f = strings.TrimLeft(f, "# ")
		f = strings.Replace(f, " ", "_", -1)

		switch f {
		case "entity":
			e = true
		case "attribute":
			a = true
		case "value":
			v = true
		}

		// Alias
		if f == "valid_time" {
			f = "time"
		}

		h[f] = i
	}

	if !e || !a || !v {
		return nil, ErrRequiredFields
	}

	return h, nil
}

// isEmptyRecord returns true if all of the values are empty strings.
func isEmptyRecord(r []string) bool {
	for _, v := range r {
		if v != "" {
			return false
		}
	}

	return true
}

type CSVReader struct {
	reader *csv.Reader
	header map[string]int
	fact   *origins.Fact
	err    error
}

func (r *CSVReader) parse(record []string) (*origins.Fact, error) {
	// Map row values to fact fields.
	var (
		ok        bool
		err       error
		idx, rlen int
		val       string
		dom       string
		f         = origins.Fact{}
	)

	rlen = len(record)

	// Domain
	if idx, ok = r.header["domain"]; ok && idx < rlen {
		f.Domain = record[idx]
	}

	// Operation
	if idx, ok = r.header["operation"]; ok && idx < rlen {
		op, err := origins.ParseOperation(record[idx])

		if err != nil {
			logrus.Error(err)
			return nil, err
		}

		f.Operation = op
	}

	// Valid time
	if idx, ok = r.header["time"]; ok && idx < rlen {
		val = record[idx]

		if val != "" {
			t, err := chrono.Parse(val)

			if err != nil {
				logrus.Error(err)
				return nil, err
			}

			f.Time = t
		}
	}

	var ident *origins.Ident

	// Entity
	idx, _ = r.header["entity"]
	val = record[idx]

	if idx, ok = r.header["entity_domain"]; ok && idx < rlen {
		dom = record[idx]
	} else {
		dom = ""
	}

	if ident, err = origins.NewIdent(dom, val); err != nil {
		return nil, err
	}

	f.Entity = ident

	// Attribute
	idx, _ = r.header["attribute"]
	val = record[idx]

	if idx, ok = r.header["attribute_domain"]; ok && idx < rlen {
		dom = record[idx]
	} else {
		dom = ""
	}

	if ident, err = origins.NewIdent(dom, val); err != nil {
		return nil, err
	}

	f.Attribute = ident

	// Value
	idx, _ = r.header["value"]
	val = record[idx]

	if idx, ok = r.header["value_domain"]; ok && idx < rlen {
		dom = record[idx]
	} else {
		dom = ""
	}

	if ident, err = origins.NewIdent(dom, val); err != nil {
		return nil, err
	}

	f.Value = ident

	return &f, nil
}

// scan reads the CSV file for the next fact.
func (r *CSVReader) next() (*origins.Fact, error) {
	var (
		err    error
		record []string
		fact   *origins.Fact
	)

	// Logic defined in a loop to skip comments.
	for {
		record, err = r.reader.Read()

		if err != nil {
			break
		}

		// Line with all empty strings.
		if isEmptyRecord(record) {
			continue
		}

		if record[0] != "" && record[0][0] == '#' {
			continue
		}

		// Parse first non-comment line as header
		if r.header == nil {
			h, err := parseHeader(record)

			if err != nil {
				break
			}

			r.header = h

			continue
		}

		fact, err = r.parse(record)

		break
	}

	if err != nil && err != io.EOF {
		logrus.Error("csv:", err)
	}

	return fact, err
}

// Next returns the next fact in the stream.
func (r *CSVReader) Next() *origins.Fact {
	if r.err != nil {
		return nil
	}

	// Scan for the next fact and queue it.
	f, err := r.next()

	// Error reading the next fact.
	if err != nil {
		r.err = err
		return nil
	}

	return f
}

// Err returns the error produced while reading.
func (r *CSVReader) Err() error {
	// EOF is an implementation detail of the underlying stream.
	if r.err == io.EOF {
		return nil
	}

	return r.err
}

// Subscribe satisfies the Publisher interface. It returns a channel of facts that
// can be consumed by downstream consumers.
func (r *CSVReader) Subscribe(done <-chan struct{}) (<-chan *origins.Fact, <-chan error) {
	ch := make(chan *origins.Fact)
	errch := make(chan error)

	go func() {
		defer func() {
			close(ch)
			close(errch)
		}()

		var (
			f   *origins.Fact
			err error
		)

		for {
			select {
			// Upstream consumer is done.
			case <-done:
				return

			default:
				if f = r.Next(); f != nil {
					ch <- f
					continue
				}

				// Signal the error if one occurred.
				if err = r.Err(); err != nil {
					errch <- err
				}

				return
			}
		}
	}()

	return ch, errch
}

func NewCSVReader(r io.Reader) *CSVReader {
	cr := csv.NewReader(r)

	// Set CSV parameters
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	// Fixed set of fields are required, however they are variable based
	// on the header.
	cr.FieldsPerRecord = -1

	return &CSVReader{
		reader: cr,
	}
}

type CSVWriter struct {
	writer  *csv.Writer
	started bool
}

func (w *CSVWriter) Write(f *origins.Fact) error {
	if !w.started {
		w.writer.Write(csvHeader)
		w.started = true
	}

	// Encode empty transaction as a empty string.
	var txs string

	if f.Transaction > 0 {
		txs = fmt.Sprint(f.Transaction)
	}

	w.writer.Write([]string{
		f.Domain,
		f.Operation.String(),
		txs,
		chrono.Format(f.Time),
		f.Entity.Domain,
		f.Entity.Name,
		f.Attribute.Domain,
		f.Attribute.Name,
		f.Value.Domain,
		f.Value.Name,
	})

	return w.writer.Error()
}

func (w *CSVWriter) Flush() error {
	w.writer.Flush()
	return w.writer.Error()
}

func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{
		writer: csv.NewWriter(w),
	}
}
