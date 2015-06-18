package origins

// Iterator is an interface that reads from an underlying stream of facts
// and makes facts available through the Next() method. The usage of the
// this interface is as follows:
//
//		// The fact will be nil if the underlying stream is exhaused
//		// or an error occurred.
//		for f := it.Next(); f != nil {
//			// Do something with the fact
//		}
//
//		if err := it.Err(); err != nil {
//			// Handle the error.
//		}
//
type Iterator interface {
	// Next returns the next available fact in the stream.
	Next() *Fact

	// Err returns an error if one occurred while iterating facts.
	Err() error
}

// Writer is an interface that defines the Write method. It takes a fact
// and writes it to the underlying value.
type Writer interface {
	// Write writes a fact to the underlying stream.
	Write(*Fact) error
}

// Flusher is an interface that defines the Flush method. Types that
// are buffered and require being *flushed* at the end of processing
// implement this method.
type Flusher interface {
	Flush() error
}

// Read consumes up to len(buf) facts of the iterator and puts them into a buffer.
func Read(it Iterator, buf Facts) (int, error) {
	var (
		i int
		f *Fact
	)

	// Range over the buffer and fill with facts.
	for _ = range buf {
		if f = it.Next(); f == nil {
			break
		}

		buf[i] = f
		i++
	}

	return i, it.Err()
}

// ReadAll reads all facts from the reader.
func ReadAll(it Iterator) (Facts, error) {
	buf := NewBuffer(nil)

	if _, err := Copy(it, buf); err != nil {
		return nil, err
	}

	return buf.Facts(), nil
}

// Copy reads all facts from the reader and copies them to the writer.
// The number of facts written is returned and an error if present.
func Copy(it Iterator, w Writer) (int, error) {
	var (
		n   int
		f   *Fact
		err error
	)

	for {
		if f = it.Next(); f == nil {
			break
		}

		if err = w.Write(f); err != nil {
			break
		}

		n++
	}

	// If a write error did not occur, set the read error.
	if err != nil {
		err = it.Err()
	}

	// If the writer implements Flusher, flush it.
	switch x := w.(type) {
	case Flusher:
		err = x.Flush()
	}

	return n, err
}
