// Counter based functions for consistently encoding/decoding integers.
package engine

import "encoding/binary"

// EncodeCounter encodes an uint64 into bytes.
func EncodeCounter(n uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, n)
	return buf
}

// DecodeCounter decodes bytes into a uint64.
func DecodeCounter(buf []byte) uint64 {
	if buf == nil {
		return 0
	}

	return binary.LittleEndian.Uint64(buf)
}
