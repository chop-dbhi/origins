/*
This module implements a CSV reader for reading facts from CSV-like formatted data.

The following fields are supported:

- Domain - The domain the fact will be asserted in. This is only required for bulk-formatted data, otherwise it is ignored.
- Operation - The operation to apply to this fact. Optional, defaults to "assert".
- Valid Time - The time this fact should be considered valid. This is separate from the "database time" which denotes when the fact was physically added. Optional, defaults to "now".
- Entity Domain - The domain of the entity. Optional, defaults to the fact domain.
- Entity - The local name of the attribute.
- Attribute Domain - The domain of the attribute. Optional, defaults to the fact domain.
- Attribute - The local name of the attribute.
- Value Domain - The domain of the value. Optional, defaults to the fact domain.
- Value - The local name of the value.

As noted, most of these fields are optional so they do not need to be included in the file. To do this, a header must be present using the above names to denote the field a column corresponds to. For example, this is a valid file:

	entity,attribute,value
	bill,likes,soccer
	bill,likes,fútbol
	soccer,is,fútbol

Applying this to the domain "sports", this will expand out to:

	domain,operation,valid time,entity domain,entity,attribute domain,attribute,value domain,value
	sports,assert,now,sports,bill,sports,likes,sports,soccer
	sports,assert,now,sports,bill,sports,likes,sports,fútbol
	sports,assert,now,sports,soccer,sports,is,sports,fútbol

The time "now" will be transaction time when it is committed to the database.

At a minimum, the entity, attribute, and value fields must be present.
*/

package fact

import (
	"encoding/csv"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/identity"
	"github.com/sirupsen/logrus"
)

var (
	ErrHeaderRequired = errors.New("A header is required with field names.")
	ErrRequiredFields = errors.New("The entity, attribute, and value fields are required.")
)

var csvHeader = []string{
	"domain",
	"operation",
	"valid_time",
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
			break
		case "attribute":
			a = true
			break
		case "value":
			v = true
			break
		}

		h[f] = i
	}

	if !e || !a || !v {
		return nil, ErrRequiredFields
	}

	return h, nil
}

type csvReader struct {
	reader *csv.Reader
	header map[string]int
	idents *identity.Cache
}

func isEmpty(r []string) bool {
	for _, v := range r {
		if v != "" {
			return false
		}
	}

	return true
}

func (r *csvReader) parse(record []string) (*Fact, error) {
	// Map row values to fact fields.
	var (
		ok        bool
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
	if idx, ok = r.header["valid_time"]; ok && idx < rlen {
		val = record[idx]

		if val != "" {
			t, err := origins.ParseTime(val)

			if err != nil {
				logrus.Error(err)
				return nil, err
			}

			f.Time = t
		}
	}

	// Entity
	idx, _ = r.header["entity"]
	val = record[idx]

	if idx, ok = r.header["entity_domain"]; ok && idx < rlen {
		dom = record[idx]
	} else {
		dom = ""
	}

	f.Entity = r.idents.Add(dom, val)

	// Attribute
	idx, _ = r.header["attribute"]
	val = record[idx]

	if idx, ok = r.header["attribute_domain"]; ok && idx < rlen {
		dom = record[idx]
	} else {
		dom = ""
	}

	f.Attribute = r.idents.Add(dom, val)

	// Value
	idx, _ = r.header["value"]
	val = record[idx]

	if idx, ok = r.header["value_domain"]; ok && idx < rlen {
		dom = record[idx]
	} else {
		dom = ""
	}

	f.Value = r.idents.Add(dom, val)

	return &f, nil
}

func (r *csvReader) read() (*Fact, error) {
	var (
		err    error
		record []string
	)

	// Logic defined in a loop to skip comments.
	for {
		record, err = r.reader.Read()

		if err != nil {
			return nil, err
		}

		// Line with all empty strings.
		if isEmpty(record) {
			continue
		}

		if record[0] != "" && record[0][0] == '#' {
			continue
		}

		// Parse first non-comment line as header
		if r.header == nil {
			h, err := parseHeader(record)

			if err != nil {
				return nil, err
			}

			r.header = h

			continue
		}

		return r.parse(record)
	}
}

// Read satisfies the fact.Reader interface.
func (r *csvReader) Read(facts Facts) (int, error) {
	var (
		f   *Fact
		err error
		l   = len(facts)
	)

	for i := 0; i < l; i++ {
		f, err = r.read()

		// EOF or error
		if err != nil {
			return i, err
		}

		// Add fact.
		if f != nil {
			facts[i] = f
		}
	}

	return l, nil
}

func CSVReader(reader io.Reader) *csvReader {
	cr := csv.NewReader(reader)

	// Set CSV parameters
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	// Fixed set of fields are required, however they are variable based
	// on the header.
	cr.FieldsPerRecord = -1

	cache := make(identity.Cache)

	r := csvReader{
		reader: cr,
		idents: &cache,
	}

	return &r
}

type csvWriter struct {
	writer *csv.Writer
	head   bool
}

func (w *csvWriter) record(f *Fact) []string {
	var time string

	if f.Time == 0 {
		time = ""
	} else {
		time = strconv.FormatInt(f.Time, 10)
	}

	return []string{
		f.Domain,
		string(f.Operation),
		time,
		f.Entity.Domain,
		f.Entity.Local,
		f.Attribute.Domain,
		f.Attribute.Local,
		f.Value.Domain,
		f.Value.Local,
	}
}

func (w *csvWriter) Write(facts Facts) (int, error) {
	// Buffer to reduce the number of calls to flush.
	var (
		n   int
		err error
	)

	if !w.head {
		w.head = true

		// Add header
		if err = w.writer.Write(csvHeader); err != nil {
			return 0, err
		}
	}

	for _, f := range facts {
		if err = w.writer.Write(w.record(f)); err != nil {
			return n, err
		}

		n += 1
	}

	// Flush and check for an error
	w.writer.Flush()

	if err = w.writer.Error(); err != nil {
		return n, err
	}

	return n, nil
}

func CSVWriter(writer io.Writer) *csvWriter {
	w := csv.NewWriter(writer)

	return &csvWriter{
		writer: w,
	}
}
