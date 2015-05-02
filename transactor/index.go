package transactor

import "github.com/chop-dbhi/origins/fact"

type IndexedFact struct {
	Domain          int
	Asserted        bool
	Time            int
	EntityDomain    int
	Entity          int
	AttributeDomain int
	Attribute       int
	ValueDomain     int
	Value           int
}

type Index struct {
	DN int
	EN int
	AN int
	VN int
	TN int

	Domains    map[string]int
	Entities   map[string]int
	Attributes map[string]int
	Values     map[string]int
	Times      map[int64]int
}

// Add a fact to the index
func (idx *Index) Add(f *fact.Fact) *IndexedFact {
	return &IndexedFact{
		Domain:          idx.SetDomain(f.Domain),
		Time:            idx.SetTime(f.Time),
		Asserted:        idx.SetOperation(f.Operation),
		EntityDomain:    idx.SetDomain(f.Entity.Domain),
		Entity:          idx.SetEntity(f.Entity.Local),
		AttributeDomain: idx.SetDomain(f.Attribute.Domain),
		Attribute:       idx.SetAttribute(f.Attribute.Local),
		ValueDomain:     idx.SetDomain(f.Value.Domain),
		Value:           idx.SetValue(f.Value.Local),
	}
}

func (idx *Index) SetOperation(s fact.Operation) bool {
	if s == fact.AssertOp {
		return true
	}

	return false
}

func (idx *Index) SetDomain(s string) int {
	var i int

	if i, ok := idx.Domains[s]; !ok {
		i = idx.DN
		idx.Domains[s] = i
		idx.DN++
	}

	return i
}

func (idx *Index) SetEntity(s string) int {
	var i int

	if i, ok := idx.Entities[s]; !ok {
		i = idx.EN
		idx.Entities[s] = i
		idx.EN++
	}

	return i
}

func (idx *Index) SetAttribute(s string) int {
	var i int

	if i, ok := idx.Attributes[s]; !ok {
		i = idx.AN
		idx.Attributes[s] = i
		idx.AN++
	}

	return i
}

func (idx *Index) SetValue(s string) int {
	var i int

	if i, ok := idx.Values[s]; !ok {
		i = idx.VN
		idx.Values[s] = i
		idx.VN++
	}

	return i
}

func (idx *Index) SetTime(s int64) int {
	var i int

	if i, ok := idx.Times[s]; !ok {
		i = idx.TN
		idx.Times[s] = i
		idx.TN++
	}

	return i
}

func NewIndex() *Index {
	return &Index{
		Domains:    make(map[string]int),
		Entities:   make(map[string]int),
		Attributes: make(map[string]int),
		Values:     make(map[string]int),
		Times:      make(map[int64]int),
	}
}
