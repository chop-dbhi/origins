package storage

import (
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
)

func BenchmarkProtoMarshal(b *testing.B) {
	domain := "test"

	e := identity.New(domain, "bill")
	a := identity.New(domain, "is")
	v := identity.New("", "tall")

	f := fact.Assert(e, a, v)

	for i := 0; i < b.N; i++ {
		MarshalProto(f)
	}
}
