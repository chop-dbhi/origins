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
	"strings"

	"github.com/chop-dbhi/origins/identity"
	"github.com/sirupsen/logrus"
)

var (
	ErrHeaderRequired = errors.New("A header is required with field names.")
	ErrRequiredFields = errors.New("The entity, attribute, and value fields are required.")
)

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

func (r *csvReader) Read() (*Fact, error) {
	var (
		err    error
		record []string
		rlen   int
	)

	// Logic defined in a loop to skip comments.
	for {
		record, err = r.reader.Read()

		if err != nil {
			return nil, err
		}

		// Line with all empty strings.
		if isEmpty(record) {
			logrus.Debug("skipping empty line")
			continue
		}

		rlen = len(record)

		if record[0] != "" && record[0][0] == '#' {
			logrus.Debug("skipping comment line")
			continue
		}

		// Parse first non-comment line as header
		if r.header == nil {
			h, err := parseHeader(record)

			if err != nil {
				return nil, err
			}

			r.header = h

			logrus.Debug("header found and cleaned")
			continue
		}

		// Map row values to fact fields.
		var (
			ok  bool
			idx int
			val string
			dom string
			f   = Fact{}
		)

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
				t, err := ParseTime(val)

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

		logrus.Infof("processed fact %v", &f)

		return &f, nil
	}
}

func CSVReader(reader io.Reader) (*csvReader, error) {
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

	return &r, nil
}
