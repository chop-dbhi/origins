package origins

// Iterator is an interface that defines the Next method. When the iterator
// is exhausted, the fact will be nil.
type Iterator interface {
	Next() (*Fact, error)
}
