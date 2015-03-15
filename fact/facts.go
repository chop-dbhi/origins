package fact

// Facts is a slice of facts.
type Facts []*Fact

type FactsReader struct {
	facts Facts
	pos   int
}
