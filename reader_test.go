package origins

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFactBufferSmall(t *testing.T) {
	e := &Ident{"", "bill"}
	a := &Ident{"", "likes"}
	v := &Ident{"", "kale"}

	f, _ := Assert(e, a, v)

	b := NewBuffer(Facts{f, f, f})

	buf := make(Facts, 2)

	var (
		n   int
		err error
	)

	n, err = b.Read(buf)

	assert.Equal(t, 2, n)
	assert.Equal(t, nil, err)

	n, err = b.Read(buf)

	assert.Equal(t, 1, n)

	if err != io.EOF {
		t.Error("expected EOF")
	}

	n, err = b.Read(buf)

	assert.Equal(t, 0, n)

	if err != io.EOF {
		t.Error("expected EOF")
	}
}

func TestFactBufferLarge(t *testing.T) {
	e := &Ident{"", "bill"}
	a := &Ident{"", "likes"}
	v := &Ident{"", "kale"}

	f, _ := Assert(e, a, v)

	b := NewBuffer(Facts{f, f, f})

	buf := make(Facts, 4)

	var (
		n   int
		err error
	)

	n, err = b.Read(buf)

	assert.Equal(t, 3, n)

	if err != io.EOF {
		t.Error("expected EOF")
	}

	n, err = b.Read(buf)

	assert.Equal(t, 0, n)

	if err != io.EOF {
		t.Error("expected EOF")
	}
}

func TestReadAll(t *testing.T) {
	e := &Ident{"", "bill"}
	a := &Ident{"", "likes"}
	v := &Ident{"", "kale"}

	f, _ := Assert(e, a, v)

	b := NewBuffer(Facts{f, f, f})

	buf, err := ReadAll(b)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, len(buf))
}

func TestMultiReader(t *testing.T) {
	e := &Ident{"", "bill"}
	a := &Ident{"", "likes"}
	v1 := &Ident{"", "kale"}
	v2 := &Ident{"", "pizza"}
	v3 := &Ident{"", "coffee"}

	f1, _ := Assert(e, a, v1)
	f2, _ := Assert(e, a, v2)
	f3, _ := Assert(e, a, v3)

	b1 := NewBuffer(Facts{f1, f2, f3})
	b2 := NewBuffer(Facts{f1, f2, f3})
	b3 := NewBuffer(Facts{f1, f2, f3})

	r := MultiReader(b1, b2, b3)

	buf, err := ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 9, len(buf))
}
