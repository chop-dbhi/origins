package view

import (
	"encoding/json"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
	"github.com/chop-dbhi/origins/storage"
)

type Map map[string][]*identity.Ident

func (m Map) MarshalJSON() ([]byte, error) {
	attrs := make(map[string]interface{}, len(m))

	var (
		i    int
		id   *identity.Ident
		val  interface{}
		vals []string
	)

	for k, ids := range m {
		if len(ids) == 1 {
			val = ids[0].String()
		} else {
			vals = make([]string, len(ids))

			for i, id = range ids {
				vals[i] = id.String()
			}

			val = vals
		}

		attrs[k] = val
	}

	return json.Marshal(attrs)
}

type Aggregate struct {
	Ident  *identity.Ident
	Domain string
	Min    int64
	Max    int64

	store *storage.Store
	attrs Map
}

// reader returns a Store reader bound by the min and max time values.
func (a *Aggregate) reader() *storage.Reader {
	r, err := a.store.RangeReader(a.Domain, a.Min, a.Max)

	if err != nil {
		panic(err)
	}

	return r
}

func (a *Aggregate) Map() Map {
	if a.attrs != nil {
		return a.attrs
	}

	m := make(Map)
	r := a.reader()

	// Add filter for this aggregate.
	r.Filter = func(f *fact.Fact) bool {
		return f.Entity.Is(a.Ident)
	}

	facts, _ := fact.ReadAll(r)

	var (
		i           int
		ok          bool
		pos         int
		attr        string
		f           *fact.Fact
		v           *identity.Ident
		_vals, vals []*identity.Ident
	)

	for _, f = range facts {
		attr = f.Attribute.String()

		// New attribute,
		if vals, ok = m[attr]; !ok {
			if f.Operation == fact.AssertOp {
				m[attr] = []*identity.Ident{f.Value}
			}
		} else {
			pos = -1

			for i, v = range vals {
				if f.Value.String() == v.String() {
					pos = i
					break
				}
			}

			if pos >= 0 {
				// Remove it
				if f.Operation == fact.RetractOp {
					_vals = vals[:pos]
					m[attr] = append(_vals, vals[pos+1:]...)
				}
			} else {
				vals = append(vals, f.Value)
				m[attr] = vals
			}
		}
	}

	a.attrs = m

	return m
}
