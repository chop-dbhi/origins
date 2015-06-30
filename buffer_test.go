package origins

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuffer(t *testing.T) {
	assert := assert.New(t)

	e := &Ident{"", "bill"}
	a := &Ident{"", "likes"}
	v1 := &Ident{"", "kale"}
	v2 := &Ident{"", "pizza"}
	v3 := &Ident{"", "coffee"}

	f0 := &Fact{
		Entity:    e,
		Attribute: a,
		Value:     v1,
	}

	f1 := &Fact{
		Entity:    e,
		Attribute: a,
		Value:     v2,
	}

	f2 := &Fact{
		Entity:    e,
		Attribute: a,
		Value:     v3,
	}

	facts := Facts{f0, f1, f2}

	buf := NewBuffer(facts)

	// Internal size defaults to the len of the passed buffer.
	// Write position is set to the len of the buffer.
	assert.Equal(3, len(buf.buf))
	assert.Equal(0, buf.off)
	assert.Equal(3, buf.Len())

	// Read the next fact.
	f := buf.Next()
	assert.Equal(f0, f)
	assert.Equal(1, buf.off)

	// One fact read, 2 remain
	assert.Equal(2, buf.Len())

	// Append another fact. The contents is shifted so the
	// internal size stays the same.
	buf.Append(f0)
	assert.Equal(3, buf.Len())
	assert.Equal(3, len(buf.buf))

	// Truncate the internal buffer to only include unread facts.
	buf.Truncate(1)
	assert.Equal(1, buf.Len())
	assert.Equal(1, len(buf.buf))
	assert.Equal(0, buf.off)

	// Confirm.
	f = buf.Next()
	assert.Equal(1, buf.off)
	assert.Equal(f1, f)

	// Buffer is consumed, buffer is truncated.
	f = buf.Next()
	assert.Equal(0, buf.off)

	if f != nil {
		t.Error("expected nil on next")
	}

	// Append beyond internal size, it should at least hold the
	// number of passed values.
	buf.Append(f0, f1, f2, f0, f1)
	assert.Equal(5, buf.Len())
	assert.Equal(5, len(buf.buf))
	assert.Equal(0, buf.off)

	// Return a copy of the unread facts.
	facts = buf.Facts()
	assert.Equal(Facts{f0, f1, f2, f0, f1}, facts)
	assert.Equal(0, buf.Len())
	assert.Equal(0, buf.off)

	// No facts returned.
	facts = buf.Facts()
	assert.Equal(Facts{}, facts)

	// Append and copy.
	buf.Append(f0)
	facts = buf.Facts()
	assert.Equal(Facts{f0}, facts)
}
