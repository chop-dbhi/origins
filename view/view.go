package view

import (
	"time"

	"github.com/chop-dbhi/origins/storage"
)

// View provides methods for computing aggregates on the store.
type View struct {
	Min   int64
	Max   int64
	store *storage.Store
}

// Domain returns a sub-view for this domain.
func (v *View) Domain(n string) *Domain {
	return &Domain{
		Name: n,
		Min:  v.Min,
		Max:  v.Max,

		store: v.store,
	}
}

// Range returns a view with an aribitrary time range.
func Range(s *storage.Store, min, max int64) *View {
	// Explicit upper bound if not set.
	if max <= 0 {
		max = time.Now().UnixNano()
	}

	return &View{
		Min:   min,
		Max:   max,
		store: s,
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
