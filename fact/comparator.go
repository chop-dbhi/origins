package fact

import "github.com/chop-dbhi/origins/identity"

// Comparator is a function type for comparing two facts.
type Comparator func(f1, f2 *Fact) bool

const (
	True int = iota - 1
	Equal
	False
)

// identComparator compares two identity.Ident values by domain then local name.
// This comparator is used as the basis for other identity-based comparators and
// therefore returns True, False or Equal with whether the left is less than the
// right.
func identComparator(i1, i2 *identity.Ident) int {
	// Compare pointer values. No change.
	if i1 == i2 {
		return Equal
	}

	// First check does not pass.
	if i1.Domain > i2.Domain {
		return False
	} else if i1.Domain < i2.Domain {
		return True
	}

	if i1.Local > i2.Local {
		return False
	} else if i1.Local < i2.Local {
		return True
	}

	return Equal
}

// EntityComparator compares two entity identities.
func EntityComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Entity, f2.Entity) {
	case True:
		return true
	default:
		return false
	}
}

// AttributeComparator compares two attribute identities.
func AttributeComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Attribute, f2.Attribute) {
	case True:
		return true
	default:
		return false
	}
}

// ValueComparator compares two value identities.
func ValueComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Value, f2.Value) {
	case True:
		return true
	default:
		return false
	}
}

// TimeComparator compares to timestamps.
func TimeComparator(f1, f2 *Fact) bool {
	return f1.Time < f2.Time
}

// EAVTComparator compares two facts using an entity-attribute-value-time sort.
func EAVTComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Entity, f2.Entity) {
	case True:
		return true
	case False:
		return false
	}

	switch identComparator(f1.Attribute, f2.Attribute) {
	case True:
		return true
	case False:
		return false
	}

	switch identComparator(f1.Value, f2.Value) {
	case True:
		return true
	case False:
		return false
	}

	return f1.Time < f1.Time
}

// AEVTComparator compares two facts using an attribute-entity-value-time sort.
func AEVTComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Attribute, f2.Attribute) {
	case True:
		return true
	case False:
		return false
	}

	switch identComparator(f1.Entity, f2.Entity) {
	case True:
		return true
	case False:
		return false
	}

	switch identComparator(f1.Value, f2.Value) {
	case True:
		return true
	case False:
		return false
	}

	return f1.Time < f1.Time
}

// AVETComparator compares two facts using an attribute-value-entity-time sort.
func AVETComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Attribute, f2.Attribute) {
	case True:
		return true
	case False:
		return false
	}

	switch identComparator(f1.Value, f2.Value) {
	case True:
		return true
	case False:
		return false
	}

	switch identComparator(f1.Entity, f2.Entity) {
	case True:
		return true
	case False:
		return false
	}

	return f1.Time < f1.Time
}

// VAETComparator compares two facts using an value-attribute-entity-time sort.
func VAETComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Value, f2.Value) {
	case True:
		return true
	case False:
		return false
	}

	switch identComparator(f1.Attribute, f2.Attribute) {
	case True:
		return true
	case False:
		return false
	}

	switch identComparator(f1.Entity, f2.Entity) {
	case True:
		return true
	case False:
		return false
	}

	return f1.Time < f1.Time
}
