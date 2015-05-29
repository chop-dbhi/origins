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

	f0, _ := Assert(e, a, v1)
	f1, _ := Assert(e, a, v2)
	f2, _ := Assert(e, a, v3)

	facts := Facts{f0, f1, f2}

	buf := NewBuffer(facts)

	assert.Equal(3, buf.Len())

	// Read the next fact.
	f, _ := buf.Next()
	assert.Equal(f0, f)

	// One fact read, 2 remain
	assert.Equal(2, buf.Len())

	buf.Append(f0)
	assert.Equal(3, buf.Len())

	// Truncate to the first unread fact
	buf.Truncate(1)
	assert.Equal(1, buf.Len())

	// Confirm.
	f, _ = buf.Next()
	assert.Equal(f1, f)

	// Buffer is consumed.
	f, _ = buf.Next()

	if f != nil {
		t.Error("expected nil on next")
	}
}
