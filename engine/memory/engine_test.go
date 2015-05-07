package memory

import (
	"testing"

	"github.com/chop-dbhi/origins/engine/test"
)

func TestEngine(t *testing.T) {
	e, _ := Init(nil)

	test.TestEngine(t, "memory", e)
}

func TestTx(t *testing.T) {
	e, _ := Init(nil)

	test.TestTx(t, "memory", e)
}

func BenchmarkEngineIncr(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkEngineIncr(b, "memory", e)
}

func BenchmarkTxIncr(b *testing.B) {
	e, _ := Init(nil)

	test.BenchmarkTxIncr(b, "memory", e)
}
