package view

import (
	"errors"
	"fmt"
	"time"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/storage"
)

func parseTime(t interface{}) (int64, error) {
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

	return -1, errors.New(fmt.Sprintf("could not parse %v as time", t))
}

// View provides methods for computing aggregates on the store.
type View struct {
	store *storage.Store
	t0    int64
	t1    int64
}

// Domain returns a sub-view for this domain.
func (v *View) Domain(n string) *Domain {
	return &Domain{
		Domain: n,
		t0:     v.t0,
		t1:     v.t1,
		store:  v.store,
	}
}

// Now returns a view spanning all time.
func Now(s *storage.Store) (*View, error) {
	v := View{
		store: s,
	}

	return &v, nil
}

// Asof returns a view with a maximum time boundary.
func Asof(s *storage.Store, t interface{}) (*View, error) {
	return Range(s, 0, t)
}

// Since returns a view with a minimum time boundary.
func Since(s *storage.Store, t interface{}) (*View, error) {
	return Range(s, t, 0)
}

// Range returns a view with an aribitrary time range.
func Range(s *storage.Store, d0, d1 interface{}) (*View, error) {
	var (
		t0, t1 int64
		err    error
	)

	if t0, err = parseTime(d0); err != nil {
		return nil, err
	}

	if t1, err = parseTime(d1); err != nil {
		return nil, err
	}

	// Explicit upper bound if not set.
	if t1 <= 0 {
		t1 = time.Now().Unix()
	}

	v := View{
		store: s,
		t0:    t0,
		t1:    t1,
	}

	return &v, nil
}
