package memory

import (
	"testing"

	"github.com/chop-dbhi/origins/storage/test"
)

func TestEngine(t *testing.T) {
	e, _ := Init(nil)

	test.TestEngine(t, "memory", e)
}

func TestTx(t *testing.T) {
	e, _ := Init(nil)

	test.TestTx(t, "memory", e)
}

func BenchmarkEngineGet(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkEngineGet(b, "memory", e)
}

func BenchmarkEngineSet(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkEngineSet(b, "memory", e)
}

func BenchmarkEngineDelete(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkEngineDelete(b, "memory", e)
}

func BenchmarkEngineIncr(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkEngineIncr(b, "memory", e)
}

func BenchmarkTxGet(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkTxGet(b, "memory", e)
}

func BenchmarkTxSet(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkTxSet(b, "memory", e)
}

func BenchmarkTxDelete(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkTxDelete(b, "memory", e)
}

func BenchmarkTxIncr(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkTxIncr(b, "memory", e)
}
