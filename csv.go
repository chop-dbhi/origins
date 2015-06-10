// This module implements a CSV reader for reading facts from CSV-like formatted data.
//
// The following fields are supported:
//
// - Domain - The domain the fact will be asserted in. This is only required for bulk-formatted data, otherwise it is ignored.
// - Operation - The operation to apply to this fact. Optional, defaults to "assert".
// - Valid Time - The time this fact should be considered valid. This is separate from the "database time" which denotes when the fact was physically added. Optional, defaults to "now".
// - Entity Domain - The domain of the entity. Optional, defaults to the fact domain.
// - Entity - The local name of the attribute.
// - Attribute Domain - The domain of the attribute. Optional, defaults to the fact domain.
// - Attribute - The local name of the attribute.
// - Value Domain - The domain of the value. Optional, defaults to the fact domain.
// - Value - The local name of the value.
//
// As noted, most of these fields are optional so they do not need to be included in the file. To do this, a header must be present using the above names to denote the field a column corresponds to. For example, this is a valid file:
//
// 	entity,attribute,value
// 	bill,likes,soccer
// 	bill,likes,fútbol
// 	soccer,is,fútbol
//
// Applying this to the domain "sports", this will expand out to:
//
// 	domain,operation,time,entity domain,entity,attribute domain,attribute,value domain,value
// 	sports,assert,now,sports,bill,sports,likes,sports,soccer
// 	sports,assert,now,sports,bill,sports,likes,sports,fútbol
// 	sports,assert,now,sports,soccer,sports,is,sports,fútbol
//
// The time "now" will be transaction time when it is committed to the database.
//
// At a minimum, the entity, attribute, and value fields must be present.

package origins

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

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
	fact   *Fact
	err    error
}

func (r *CSVReader) parse(record []string) (*Fact, error) {
	// Map row values to fact fields.
	var (
		ok        bool
		err       error
		idx, rlen int
		val       string
		dom       string
		f         = Fact{}
	)

	rlen = len(record)

	// Domain
	if idx, ok = r.header["domain"]; ok && idx < rlen {
		f.Domain = record[idx]
	}

	// Operation
	if idx, ok = r.header["operation"]; ok && idx < rlen {
		op, err := ParseOperation(record[idx])

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

	var ident *Ident

	// Entity
	idx, _ = r.header["entity"]
	val = record[idx]

	if idx, ok = r.header["entity_domain"]; ok && idx < rlen {
		dom = record[idx]
	} else {
		dom = ""
	}

	if ident, err = NewIdent(dom, val); err != nil {
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

	if ident, err = NewIdent(dom, val); err != nil {
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

	if ident, err = NewIdent(dom, val); err != nil {
		return nil, err
	}

	f.Value = ident

	return &f, nil
}

// scan reads the CSV file for the next fact.
func (r *CSVReader) next() (*Fact, error) {
	var (
		err    error
		record []string
		fact   *Fact
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
func (r *CSVReader) Next() *Fact {
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
func (r *CSVReader) Subscribe(done <-chan struct{}) (<-chan *Fact, <-chan error) {
	ch := make(chan *Fact)
	errch := make(chan error)

	go func() {
		defer func() {
			close(ch)
			close(errch)
		}()

		var (
			f   *Fact
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

func (w *CSVWriter) Write(f *Fact) error {
	if !w.started {
		w.writer.Write(csvHeader)
		w.started = true
	}

	w.writer.Write([]string{
		f.Domain,
		f.Operation.String(),
		fmt.Sprint(f.Transaction),
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
