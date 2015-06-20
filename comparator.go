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

// IdentComparator compares to identities.
func IdentComparator(id1, id2 *Ident) bool {
	if identComparator(id1, id2) == compTrue {
		return true
	}

	return false
}

// EntityComparator compares two entity identities.
func EntityComparator(f1, f2 *Fact) bool {
	return IdentComparator(f1.Entity, f2.Entity)
}

// AttributeComparator compares two attribute identities.
func AttributeComparator(f1, f2 *Fact) bool {
	return IdentComparator(f1.Attribute, f2.Attribute)
}

// ValueComparator compares two value identities.
func ValueComparator(f1, f2 *Fact) bool {
	return IdentComparator(f1.Value, f2.Value)
}

// TransactionComparator compares two value identities.
func TransactionComparator(f1, f2 *Fact) bool {
	return f1.Transaction < f2.Transaction
}

// TimeComparator compares two times.
func TimeComparator(f1, f2 *Fact) bool {
	return f1.Time.Before(f2.Time)
}

// EAVTComparator compares two facts using an entity-attribute-value-time sort.
func EAVTComparator(f1, f2 *Fact) bool {
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

	return TransactionComparator(f1, f2)
}

// AEVTComparator compares two facts using an attribute-entity-value-time sort.
func AEVTComparator(f1, f2 *Fact) bool {
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

	return TransactionComparator(f1, f2)
}

// AVETComparator compares two facts using an attribute-value-entity-time sort.
func AVETComparator(f1, f2 *Fact) bool {
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

	return TransactionComparator(f1, f2)
}

// VAETComparator compares two facts using an value-attribute-entity-time sort.
func VAETComparator(f1, f2 *Fact) bool {
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

	return TransactionComparator(f1, f2)
}
