package origins

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/chop-dbhi/origins/chrono"
)

// untitle takes a title-cased field and lowercases the title portion. This
// applies to field names.
func untitle(s string) string {
	b := []byte(s)

	for i, c := range b {
		if c >= 65 && c <= 90 {
			b[i] = c + 32
		} else {
			break
		}
	}

	return string(b)
}

// Identifier defines the Ident method that returns and Ident value.
// Types that implement will used during reflection to be properly
// represented as facts.
type Identifier interface {
	Ident() *Ident
}

type fieldTag struct {
	AttrName    string
	AttrDomain  string
	ValueDomain string
}

func parseFieldTag(f *reflect.StructField) *fieldTag {
	t := new(fieldTag)

	// Evaluate tag.
	toks := strings.Split(f.Tag.Get("origins"), ",")

	if len(toks) == 0 {
		return t
	}

	// Omit the field
	if toks[0] == "-" {
		return nil
	}

	// First token is attribute name.
	if toks[0] != "" {
		t.AttrName = toks[0]
	}

	// Explicit attribute domain.
	if len(toks) > 1 {
		t.AttrDomain = toks[1]
	}

	// Explicit value domain for reference.
	if len(toks) > 2 {
		t.ValueDomain = toks[2]
	}

	if len(toks) > 3 {
		logrus.Panic("attribute name, attribute domain, value domain tags are supported")
	}

	return t
}

func reflectValue(f *reflect.StructField, v reflect.Value) *Ident {
	ident := new(Ident)

	// Evaluate primitive types.
	switch f.Type.Kind() {
	case reflect.String:
		ident.Name = v.String()

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		ident.Name = fmt.Sprint(v.Int())

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		ident.Name = fmt.Sprint(v.Uint())

	case reflect.Float32, reflect.Float64:
		ident.Name = fmt.Sprint(v.Float())

	case reflect.Bool:
		ident.Name = fmt.Sprint(v.Bool())

	case reflect.Complex64, reflect.Complex128:
		ident.Name = fmt.Sprint(v.Complex())

	case reflect.Struct:
		// Check for interface and custom types.
		switch x := v.Interface().(type) {
		case Identifier:
			ident = x.Ident()

		case time.Time:
			ident.Name = chrono.Format(x)

		case fmt.Stringer:
			ident.Name = x.String()
		}

	case reflect.Interface, reflect.Ptr:
		if v.IsNil() {
			return nil
		}

		// Check for interface and custom types.
		switch x := v.Interface().(type) {
		case Identifier:
			ident = x.Ident()

		case time.Time:
			ident.Name = chrono.Format(x)

		case fmt.Stringer:
			ident.Name = x.String()
		}

	default:
		logrus.Debugf("origins: skipping unsupported field %s (%s type)", f.Name, f.Type.Kind())
		return nil
	}

	return ident
}

// Reflect takes a value and returns a set of partially defined facts
// containing attribute and value components. Currently, struct values or
// pointers to struct values are supported. Struct fields with a primitive
// type are included (support for pointers coming soon). The `origins` tag
// can be used to specify an alternate identity name, an attribute domain
// name or omit the field all together.
func Reflect(v interface{}) (Facts, error) {
	typ := reflect.TypeOf(v)
	val := reflect.ValueOf(v)

	// Deference pointers.
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		val = reflect.Indirect(val)
	}

	// Only structs are supported.
	if typ.Kind() != reflect.Struct {
		logrus.Panicf("origins: facts cannot be generated from %s types", typ.Kind())
	}

	var (
		ft                  *fieldTag
		sf                  reflect.StructField
		facts               []*Fact
		entity, attr, value *Ident
	)

	// Get the entity identifier from the value.
	switch x := v.(type) {
	case Identifier:
		entity = x.Ident()
	}

	// Iterate fields by index.
	for i := 0; i < typ.NumField(); i++ {
		sf = typ.Field(i)

		// Non-empty package denotes an unexported field.
		if sf.PkgPath != "" {
			logrus.Debugf("origins: skipping unexported field %s", sf.Name)
			continue
		}

		ft = parseFieldTag(&sf)

		// Nil denotes the field is omitted.
		if ft == nil {
			continue
		}

		// No custom attr name specified.
		if ft.AttrName == "" {
			ft.AttrName = untitle(sf.Name)
		}

		value = reflectValue(&sf, val.Field(i))

		// No value could be inferred.
		if value == nil {
			continue
		}

		value.Domain = ft.ValueDomain

		attr = &Ident{
			Domain: ft.AttrDomain,
			Name:   ft.AttrName,
		}

		facts = append(facts, &Fact{
			Entity:    entity,
			Attribute: attr,
			Value:     value,
		})
	}

	return facts, nil
}
