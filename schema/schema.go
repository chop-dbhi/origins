// The schema package defines types for representing the optional schema support
// in Origins. Schemata are defined like ordinary domains; they materialize as a
// composition of facts and are tracked over time. In practice this means that
// the semantics a set of facts must adhere to is relative to the time they are
// transacted. For example, if an attribute's type changes, all facts transacted
// before that time are unaffected. The recommended approach to handling a schema
// change is to perform a migration of the existing facts which involves retracting
// the existing facts and asserting a new set.
// TODO: Add functions for performing schema migrations.
package schema

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/storage"
	"github.com/chop-dbhi/origins/view"
	"github.com/sirupsen/logrus"
)

// Type defines the data type of a value set for an attribute.
type Type uint8

func (t Type) String() string {
	switch t {
	case String:
		return "string"

	case Int:
		return "int"

	case Uint:
		return "uint"

	case Float:
		return "float"

	case Bool:
		return "bool"

	case Time:
		return "time"

	case Ref:
		return "ref"
	}

	return ""
}

func (t Type) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

const (
	String Type = iota
	Int
	Uint
	Float
	Bool
	Time
	Ref
)

// Map of string to type.
var typeMap = map[string]Type{
	"string": String,
	"int":    Int,
	"uint":   Uint,
	"float":  Float,
	"bool":   Bool,
	"time":   Time,
	"ref":    Ref,
}

// Cardinality defines the number of values that can be set
// for an attribute at one time.
type Cardinality uint8

func (c Cardinality) String() string {
	switch c {
	case One:
		return "one"
	case Many:
		return "many"
	}

	return ""
}

func (c Cardinality) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.String())
}

const (
	One Cardinality = iota
	Many
)

// Attribute declares descriptive and functional properties which are applied
// to facts in various contexts. For example, if an attribute delcares a type
// of integer, but the associated value in a fact is not an integer (or cannot
// be coerced), then the fact would fail validation.
type Attribute struct {
	Domain      string
	Name        string
	Label       string
	Doc         string
	Type        Type
	Cardinality Cardinality
	Unique      bool
}

func (a *Attribute) String() string {
	return fmt.Sprintf("%s/%s", a.Domain, a.Name)
}

// A Schema defines the semantics of a set of entities that are used as
// attributes in other domains. Schema entities define one or more of
// the built-in schema attributes to refine the behavior during validation.
type Schema struct {
	Domain string
	attrs  map[[2]string]*Attribute
}

// Get gets an attribute in the schema or nil if it does not exist.
func (s *Schema) Get(domain, name string) *Attribute {
	return s.attrs[[2]string{domain, name}]
}

// Add adds an attribute to the schema.
func (s *Schema) Add(attr *Attribute) {
	s.attrs[[2]string{attr.Domain, attr.Name}] = attr
}

// Attrs returns a slice of attributes.
func (s *Schema) Attrs() []*Attribute {
	attrs := make([]*Attribute, len(s.attrs))

	var i int

	for _, a := range s.attrs {
		attrs[i] = a
		i++
	}

	return attrs
}

// Facts encodes the schema as facts. This is primarily used for programmatically
// creating a schema and then writing the facts to storage for later use.
func (s *Schema) Facts() origins.Facts {
	var (
		err    error
		attr   *Attribute
		fact   *origins.Fact
		facts  origins.Facts
		buf    = origins.NewBuffer(nil)
		entity *origins.Ident
	)

	for _, attr = range s.attrs {
		entity = &origins.Ident{
			Domain: attr.Domain,
			Name:   attr.Name,
		}

		if facts, err = origins.Reflect(attr); err != nil {
			panic(err)
		}

		for _, fact = range facts {
			fact.Domain = s.Domain
			fact.Entity = entity

			// Fill in the defaults.
			if fact.Attribute.Domain == "" {
				fact.Attribute.Domain = fact.Domain
			}

			buf.Write(fact)
		}
	}

	return buf.Facts()
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	aux := map[string]interface{}{
		"Domain": s.Domain,
		"Attrs":  s.Attrs(),
	}

	return json.Marshal(aux)
}

// Init initializes a schema from the passed iterator.
func Init(domain string, iter origins.Iterator) *Schema {
	var (
		attr *Attribute

		schema = &Schema{
			Domain: domain,
			attrs:  make(map[[2]string]*Attribute),
		}
	)

	origins.Map(iter, func(f *origins.Fact) error {
		// Ignore facts that are not defined in the same domain.
		if f.Domain == "" {
			f.Domain = domain
		}

		if f.Entity.Domain == "" {
			f.Entity.Domain = f.Domain
		}

		if f.Attribute.Domain == "" {
			f.Attribute.Domain = f.Domain
		}

		// Schema attributes are defined under the attrs domain.
		if f.Attribute.Domain != origins.AttrsDomain {
			logrus.Debugf("schema: ignoring non-schema attribute %s", f.Attribute)
			return nil
		}

		// Empty value, ignore.
		if f.Value.Name == "" {
			logrus.Debugf("schema: ignoring empty value for %s", f.Attribute)
			return nil
		}

		// Get the schema attribute for this name.
		if attr = schema.Get(f.Entity.Domain, f.Entity.Name); attr == nil {
			attr = &Attribute{
				Domain: f.Entity.Domain,
				Name:   f.Entity.Name,
			}

			schema.Add(attr)
		}

		switch f.Attribute.Name {
		case "label":
			attr.Label = f.Value.Name

		case "doc":
			attr.Doc = f.Value.Name

		case "unique":
			attr.Unique = strings.ToLower(f.Value.Name) == "true"

		case "cardinality":
			if f.Value.Domain == origins.CardinalitiesDomain {
				if strings.ToLower(f.Value.Name) == "many" {
					attr.Cardinality = Many
				} else {
					attr.Cardinality = One
				}
			} else {
				logrus.Debugf("schema: invalid cardinality option %s", f.Value)
			}

		case "type":
			attr.Type = typeMap[strings.ToLower(f.Value.Name)]
		}

		return nil
	})

	return schema
}

// Load materializes the current state of a schema from the database.
func Load(engine storage.Engine, domain string) (*Schema, error) {
	log, err := view.OpenLog(engine, domain, "commit")

	if err != nil {
		return nil, err
	}

	iter := log.Now()

	return Init(domain, iter), nil
}
