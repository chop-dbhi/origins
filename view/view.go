package view

import (
	"time"

	"github.com/chop-dbhi/origins/storage"
)

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
