package origins

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFactBufferSmall(t *testing.T) {
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v, _ := NewIdent("", "kale")

	facts := Facts{
		Assert(e, a, v),
		Assert(e, a, v),
		Assert(e, a, v),
	}

	b := NewBuffer(facts)

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
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v, _ := NewIdent("", "kale")

	facts := Facts{
		Assert(e, a, v),
		Assert(e, a, v),
		Assert(e, a, v),
	}

	b := NewBuffer(facts)

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
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v, _ := NewIdent("", "kale")

	facts := Facts{
		Assert(e, a, v),
		Assert(e, a, v),
		Assert(e, a, v),
	}

	b := NewBuffer(facts)

	buf, err := ReadAll(b)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, len(buf))
}

func TestMultiReader(t *testing.T) {
	e, _ := NewIdent("", "bill")
	a, _ := NewIdent("", "likes")
	v1, _ := NewIdent("", "kale")
	v2, _ := NewIdent("", "pizza")
	v3, _ := NewIdent("", "coffee")

	facts := Facts{
		Assert(e, a, v1),
		Assert(e, a, v2),
		Assert(e, a, v3),
	}

	b1 := NewBuffer(facts)
	b2 := NewBuffer(facts)
	b3 := NewBuffer(facts)

	r := MultiReader(b1, b2, b3)

	buf, err := ReadAll(r)

	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 9, len(buf))
}
