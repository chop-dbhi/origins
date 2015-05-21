package origins

// Comparator is a function type that compares two facts for the purposes of sorting.
// It returns true if the first fact should come before the second fact.
type Comparator func(f1, f2 *Fact) bool

const (
	compTrue int8 = iota - 1
	compEqual
	compFalse
)

// identComparator compares two Ident values by domain then local name.
// This comparator is used as the basis for other identity-based comparators and
// therefore returns compTrue, compFalse or compEqual with whether the left is less than the
// right.
func identComparator(i1, i2 *Ident) int8 {
	// Compare pointer values. No change.
	if i1 == i2 {
		return compEqual
	}

	// First check does not pass.
	if i1.Domain > i2.Domain {
		return compFalse
	} else if i1.Domain < i2.Domain {
		return compTrue
	}

	if i1.Name > i2.Name {
		return compFalse
	} else if i1.Name < i2.Name {
		return compTrue
	}

	return compEqual
}

// entityComparator compares two entity identities.
func entityComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Entity, f2.Entity) {
	case compTrue:
		return true
	default:
		return false
	}
}

// attributeComparator compares two attribute identities.
func attributeComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Attribute, f2.Attribute) {
	case compTrue:
		return true
	default:
		return false
	}
}

// valueComparator compares two value identities.
func valueComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Value, f2.Value) {
	case compTrue:
		return true
	default:
		return false
	}
}

// timeComparator compares two times.
func timeComparator(f1, f2 *Fact) bool {
	return f1.Time.Before(f2.Time)
}

// eavtComparator compares two facts using an entity-attribute-value-time sort.
func eavtComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Entity, f2.Entity) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	switch identComparator(f1.Attribute, f2.Attribute) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	switch identComparator(f1.Value, f2.Value) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	return timeComparator(f1, f2)
}

// aevtComparator compares two facts using an attribute-entity-value-time sort.
func aevtComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Attribute, f2.Attribute) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	switch identComparator(f1.Entity, f2.Entity) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	switch identComparator(f1.Value, f2.Value) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	return timeComparator(f1, f2)
}

// avetComparator compares two facts using an attribute-value-entity-time sort.
func avetComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Attribute, f2.Attribute) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	switch identComparator(f1.Value, f2.Value) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	switch identComparator(f1.Entity, f2.Entity) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	return timeComparator(f1, f2)
}

// vaetComparator compares two facts using an value-attribute-entity-time sort.
func vaetComparator(f1, f2 *Fact) bool {
	switch identComparator(f1.Value, f2.Value) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	switch identComparator(f1.Attribute, f2.Attribute) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	switch identComparator(f1.Entity, f2.Entity) {
	case compTrue:
		return true
	case compFalse:
		return false
	}

	return timeComparator(f1, f2)
}
