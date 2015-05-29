package origins

import "io"

// Buffer holds a slice of facts. The buffer dynamically grows as facts
// are written to it. The position is maintained across reads.
type Buffer struct {
	buf Facts

	// The next position in the buffer to read and write.
	rpos int
	wpos int
}

// Grow increase the buffer size by n.
func (b *Buffer) Grow(n int) {
	tmp := make(Facts, len(b.buf)+n)
	copy(tmp, b.buf)
	b.buf = tmp
}

// Len returns the length of the unread portion of the buffer.
func (b *Buffer) Len() int {
	return b.wpos - b.rpos
}

// Write takes a slice of facts and writes them to the buffer.
func (b *Buffer) Append(buf ...*Fact) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}

	l := len(b.buf) - b.wpos
	n := len(buf)

	// If the buffer is not large enough, grow it by the difference
	// in size.
	if l < n {
		b.Grow(n - l)
	}

	copy(b.buf[b.wpos:], buf)
	b.wpos += n

	return n, nil
}

// Facts returns the unread portion of facts.
func (b *Buffer) Facts() Facts {
	c := b.buf[b.rpos:]
	b.rpos = len(b.buf)
	return c
}

// Truncate discards all but the first n unread facts from the buffer.
func (b *Buffer) Truncate(n int) {
	b.buf = b.buf[b.rpos : b.rpos+n]
	b.rpos = 0

	if len(b.buf) < b.wpos {
		b.wpos = len(b.buf)
	}

}

// Reset resets the buffer so it has not content. This is equivalent to
// calling b.Truncate(0).
func (b *Buffer) Reset() {
	b.Truncate(0)
}

// Next implements the Iterator interface. It returns the
// next unread fact in the buffer.
func (b *Buffer) Next() (*Fact, error) {
	if b.rpos == b.wpos {
		return nil, io.EOF
	}

	f := b.buf[b.rpos]
	b.rpos++

	return f, nil
}

// Read implements the Reader interface.
func (b *Buffer) Read(buf Facts) (int, error) {
	// Consumed.
	if b.rpos >= b.wpos {
		return 0, io.EOF
	}

	var err error

	l := len(buf)

	lo := b.rpos
	hi := lo + l

	// Cap hi to the unread facts.
	if hi >= b.wpos {
		hi = b.wpos
		err = io.EOF
	}

	// Actual number observed.
	n := hi - lo

	if n > 0 {
		copy(buf, b.buf[lo:hi])
		b.rpos += n
	}

	return n, err
}

// NewBuffer initializes a buffer of facts.
func NewBuffer(buf Facts) *Buffer {
	if buf == nil {
		buf = make(Facts, 0)
	}

	return &Buffer{
		buf:  buf,
		wpos: len(buf),
	}
}
