package fact

import "github.com/chop-dbhi/origins/identity"

// Comparator is a function type for comparing two facts.
type Comparator func(f1, f2 *Fact) bool

// identComparator compares two identity.Ident values by domain then local name.
func identComparator(i1, i2 *identity.Ident) bool {
	// Compare pointer values.
	if i1 == i2 {
		return false
	}

	if i1.Domain < i2.Domain {
		return true
	}

	if i1.Local < i2.Local {
		return true
	}

	return false
}

// EntityComparator compares two entity identities.
func EntityComparator(f1, f2 *Fact) bool {
	return identComparator(f1.Entity, f2.Entity)
}

// AttributeComparator compares two attribute identities.
func AttributeComparator(f1, f2 *Fact) bool {
	return identComparator(f1.Attribute, f2.Attribute)
}

// ValueComparator compares two value identities.
func ValueComparator(f1, f2 *Fact) bool {
	return identComparator(f1.Value, f2.Value)
}

// TimeComparator compares to timestamps.
func TimeComparator(f1, f2 *Fact) bool {
	return f1.Time < f1.Time
}

// EAVTComparator compares two facts using an entity-attribute-value-time sort.
func EAVTComparator(f1, f2 *Fact) bool {
	if EntityComparator(f1, f2) {
		return true
	}

	if AttributeComparator(f1, f2) {
		return true
	}

	if ValueComparator(f1, f2) {
		return true
	}

	if TimeComparator(f1, f2) {
		return true
	}

	return false
}

// AEVTComparator compares two facts using an attribute-entity-value-time sort.
func AEVTComparator(f1, f2 *Fact) bool {
	if AttributeComparator(f1, f2) {
		return true
	}

	if EntityComparator(f1, f2) {
		return true
	}

	if ValueComparator(f1, f2) {
		return true
	}

	if TimeComparator(f1, f2) {
		return true
	}

	return false
}

// AVETComparator compares two facts using an attribute-value-entity-time sort.
func AVETComparator(f1, f2 *Fact) bool {
	if AttributeComparator(f1, f2) {
		return true
	}

	if ValueComparator(f1, f2) {
		return true
	}

	if EntityComparator(f1, f2) {
		return true
	}

	if TimeComparator(f1, f2) {
		return true
	}

	return false
}

// VAETComparator compares two facts using an value-attribute-entity-time sort.
func VAETComparator(f1, f2 *Fact) bool {
	if ValueComparator(f1, f2) {
		return true
	}

	if AttributeComparator(f1, f2) {
		return true
	}

	if EntityComparator(f1, f2) {
		return true
	}

	if TimeComparator(f1, f2) {
		return true
	}

	return false
}
