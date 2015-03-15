package view

import (
	"errors"
	"fmt"
	"time"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
)

func ParseTime(t interface{}) (int64, error) {
	switch x := t.(type) {
	case string:
		return fact.ParseTime(x)
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case time.Duration:
		n := time.Now()
		n = n.Add(-x)
		return n.Unix(), nil
	case time.Time:
		return x.Unix(), nil
	}

	return 0, errors.New(fmt.Sprintf("could not parse %v as time", t))
}

// View provides methods for computing aggregates on the store.
type View struct {
	store *storage.Store
	min   int64
	max   int64
}

// Domain returns a sub-view for this domain.
func (v *View) Domain(n string) *Domain {
	return &Domain{
		Name:  n,
		min:   v.min,
		max:   v.max,
		store: v.store,
	}
}

// Range returns a view with an aribitrary time range.
func Range(s *storage.Store, min, max int64) *View {
	// Explicit upper bound if not set.
	if max <= 0 {
		max = time.Now().Unix()
	}

	return &View{
		store: s,
		min:   min,
		max:   max,
	}
}

// Now returns a view spanning all time. This is a shorthand
func Now(s *storage.Store) *View {
	return Range(s, 0, 0)
}

// Asof returns a view with a maximum time boundary.
func Asof(s *storage.Store, max int64) *View {
	return Range(s, 0, max)
}

// Since returns a view with a minimum time boundary.
func Since(s *storage.Store, min int64) *View {
	return Range(s, min, 0)
}
