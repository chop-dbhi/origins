package identity

import (
	"bytes"
	"testing"
)

func cmpPtr(id1, id2 *Ident) bool {
	return id1 == id2
}

func cmpBytes(id1, id2 []byte) bool {
	return bytes.Equal(id1, id2)
}

func cmpString(id1, id2 string) bool {
	return id1 == id2
}

func benchCmpPtr(b *testing.B, n int) {
	id1 := &Ident{
		Local: string(make([]byte, n)),
	}

	id2 := id1

	for i := 0; i < b.N; i++ {
		cmpPtr(id1, id2)
	}
}

func benchCmpString(b *testing.B, n int) {
	id := Ident{
		Local: string(make([]byte, n)),
	}

	id1 := string(id.String())
	id2 := string(id.String())

	for i := 0; i < b.N; i++ {
		cmpString(id1, id2)
	}
}

func benchCmpBytes(b *testing.B, n int) {
	id := Ident{
		Local: string(make([]byte, n)),
	}

	id1 := []byte(id.String())
	id2 := []byte(id.String())

	for i := 0; i < b.N; i++ {
		cmpBytes(id1, id2)
	}
}

// Pointer
func BenchmarkCmpPtr__8(b *testing.B) {
	benchCmpPtr(b, 8)
}

func BenchmarkCmpPtr__16(b *testing.B) {
	benchCmpPtr(b, 16)
}

func BenchmarkCmpPtr__32(b *testing.B) {
	benchCmpPtr(b, 16)
}

func BenchmarkCmpPtr__64(b *testing.B) {
	benchCmpPtr(b, 16)
}

func BenchmarkCmpPtr__128(b *testing.B) {
	benchCmpPtr(b, 16)
}

// String
func BenchmarkCmpString__8(b *testing.B) {
	benchCmpString(b, 8)
}

func BenchmarkCmpString__16(b *testing.B) {
	benchCmpString(b, 16)
}

func BenchmarkCmpString__32(b *testing.B) {
	benchCmpString(b, 16)
}

func BenchmarkCmpString__64(b *testing.B) {
	benchCmpString(b, 16)
}

func BenchmarkCmpString__128(b *testing.B) {
	benchCmpString(b, 16)
}

// Bytes
func BenchmarkCmpBytes__8(b *testing.B) {
	benchCmpBytes(b, 8)
}

func BenchmarkCmpBytes__16(b *testing.B) {
	benchCmpBytes(b, 16)
}

func BenchmarkCmpBytes__32(b *testing.B) {
	benchCmpBytes(b, 16)
}

func BenchmarkCmpBytes__64(b *testing.B) {
	benchCmpBytes(b, 16)
}

func BenchmarkCmpBytes__128(b *testing.B) {
	benchCmpBytes(b, 16)
}
