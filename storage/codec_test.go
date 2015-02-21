package storage

import (
	"testing"

	"github.com/chop-dbhi/origins/fact"
	"github.com/chop-dbhi/origins/identity"
)

func codecBenchmark(b *testing.B, codec Codec) {
	domain := "test"

	e := identity.New(domain, "bill")
	a := identity.New(domain, "is")
	v := identity.New("", "tall")

	f := fact.Assert(e, a, v, nil)

	for i := 0; i < b.N; i++ {
		codec.Marshal(f)
	}
}

func BenchmarkJSONCodec(b *testing.B) {
	codecBenchmark(b, &JSONCodec{})
}

func BenchmarkBSONCodec(b *testing.B) {
	codecBenchmark(b, &BSONCodec{})
}

func BenchmarkMessagePackCodec(b *testing.B) {
	codecBenchmark(b, &MessagePackCodec{})
}

func BenchmarkProtobufCodec(b *testing.B) {
	codecBenchmark(b, &ProtobufCodec{})
}
