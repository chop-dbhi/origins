package origins

// Buffer holds a slice of facts. The buffer dynamically grows as facts
// are written to it. The position is maintained across reads.
type Buffer struct {
	buf Facts
	// Contents are buf[off : len(buf)]
	off int
}

// grow grows the buffer to guarantee space for n more facts.
// It returns the index where facts should be written.
func (b *Buffer) grow(n int) int {
	m := b.Len()

	// Buffer is empty, reset to reclaim space.
	if m == 0 && b.off != 0 {
		b.Truncate(0)
	}

	if len(b.buf)+n > cap(b.buf) {
		var buf Facts

		if m+n <= cap(b.buf)/2 {
			copy(b.buf[:], b.buf[b.off:])
			buf = b.buf[:m]
		} else {
			buf = make(Facts, 2*cap(b.buf)+n)
			copy(buf, b.buf[b.off:])
		}

		b.buf = buf
		b.off = 0
	}

	b.buf = b.buf[0 : b.off+m+n]
	return b.off + m
}

// Grow increase the buffer size by a minimum of n.
func (b *Buffer) Grow(n int) {
	b.grow(n)
}

// Len returns the length of the unread portion of the buffer.
func (b *Buffer) Len() int {
	return len(b.buf) - b.off
}

// Write writes a fact to the buffer.
func (b *Buffer) Write(f *Fact) error {
	_, err := b.Append(f)
	return err
}

// Write takes a slice of facts and writes them to the buffer.
func (b *Buffer) Append(buf ...*Fact) (int, error) {
	m := b.grow(len(buf))
	return copy(b.buf[m:], buf), nil
}

// Facts returns a copy of the unread portion of facts.
func (b *Buffer) Facts() Facts {
	n := b.Len()

	if n == 0 {
		return Facts{}
	}

	c := make(Facts, n)
	copy(c, b.buf[b.off:])

	// Reclaim space.
	b.Truncate(0)
	return c
}

// Truncate resets the read and write position to the specified index.
func (b *Buffer) Truncate(n int) {
	switch {
	case n < 0 || n > b.Len():
		panic("origins.Buffer: truncation out of range")
	case n == 0:
		// Reuse buffer space.
		b.off = 0
	}

	b.buf = b.buf[0 : b.off+n]
}

// Reset resets the buffer so it has not content. This is equivalent to
// calling b.Truncate(0).
func (b *Buffer) Reset() {
	b.Truncate(0)
}

// Next returns the next unread fact in the buffer.
func (b *Buffer) Next() *Fact {
	// Do not exceed to the write position.
	if b.off >= len(b.buf) {
		// Buffer is empty, reset.
		b.Truncate(0)

		return nil
	}

	f := b.buf[b.off]
	b.off++

	return f
}

func (b *Buffer) Err() error {
	return nil
}

// NewBuffer initializes a buffer of facts.
func NewBuffer(buf Facts) *Buffer {
	return &Buffer{
		buf: buf,
	}
}
