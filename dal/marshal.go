package dal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/chop-dbhi/origins"
	"github.com/chop-dbhi/origins/chrono"
	"github.com/golang/protobuf/proto"
	"github.com/satori/go.uuid"
	"github.com/Sirupsen/logrus"
)

// A slice of facts use length-prefix framing to delimit facts. The prefix is
// a fixed size which requires the fact itself to not exceed the number of
// bytes the prefix can encode.
//
//     [prefix] | [data] | [prefix] | [data] | ...
//
// See http://eli.thegreenplace.net/2011/08/02/length-prefix-framing-for-protocol-buffers
// for additional information.
const (
	// Size of the fact prefix for length-prefixing.
	factPrefixSize = 4

	// Maximum fact size in bytes. This is imposed due to the length-prefixing
	maxFactSize uint64 = 1 << (8 * factPrefixSize)
)

// encodeUUID encodes a UUID value into bytes.
func encodeUUID(u *uuid.UUID) []byte {
	if u == nil {
		return nil
	}

	return u.Bytes()
}

// decodeUUID decodes bytes into a UUID value.
func decodeUUID(b []byte) *uuid.UUID {
	if b == nil {
		return nil
	}

	u, err := uuid.FromBytes(b)

	if err != nil {
		panic(err)
	}

	return &u
}

func marshalLog(l *Log) ([]byte, error) {
	m := ProtoLog{
		Head: encodeUUID(l.Head),
	}

	return proto.Marshal(&m)
}

func unmarshalLog(b []byte, l *Log) error {
	m := ProtoLog{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	l.Head = decodeUUID(m.GetHead())

	return nil
}

func marshalSegment(s *Segment) ([]byte, error) {
	m := ProtoSegment{
		UUID:        encodeUUID(s.UUID),
		Transaction: proto.Uint64(s.Transaction),
		Time:        proto.Int64(chrono.TimeMicro(s.Time)),
		Blocks:      proto.Int32(int32(s.Blocks)),
		Count:       proto.Int32(int32(s.Count)),
		Bytes:       proto.Int32(int32(s.Bytes)),
		Next:        encodeUUID(s.Next),
		Base:        encodeUUID(s.Base),
	}

	return proto.Marshal(&m)
}

func unmarshalSegment(b []byte, s *Segment) error {
	m := ProtoSegment{}

	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}

	s.UUID = decodeUUID(m.GetUUID())
	s.Transaction = m.GetTransaction()
	s.Time = chrono.MicroTime(m.GetTime())
	s.Blocks = int(m.GetBlocks())
	s.Count = int(m.GetCount())
	s.Bytes = int(m.GetBytes())
	s.Next = decodeUUID(m.GetNext())
	s.Base = decodeUUID(m.GetBase())

	return nil
}

func marshalFact(m *ProtoFact, f *origins.Fact) ([]byte, error) {
	m.Reset()

	m.EntityDomain = proto.String(f.Entity.Domain)
	m.Entity = proto.String(f.Entity.Name)

	m.AttributeDomain = proto.String(f.Attribute.Domain)
	m.Attribute = proto.String(f.Attribute.Name)

	m.ValueDomain = proto.String(f.Value.Domain)
	m.Value = proto.String(f.Value.Name)

	m.Time = proto.Int64(chrono.TimeMicro(f.Time))

	switch f.Operation {
	case origins.Assertion:
		m.Added = proto.Bool(true)
	case origins.Retraction:
		m.Added = proto.Bool(false)
	default:
		panic("fact: invalid op")
	}

	return proto.Marshal(m)
}

// unmarshalFact decodes a fact from it's binary representation. The domain and
// transaction ID are passed in since they are not encoded with the fact itself.
// This is because facts are stored relative to a domain and a transaction.
func unmarshalFact(m *ProtoFact, b []byte, d string, t uint64, f *origins.Fact) error {
	m.Reset()

	if err := proto.Unmarshal(b, m); err != nil {
		return err
	}

	f.Domain = d
	f.Transaction = t

	f.Entity = &origins.Ident{
		Domain: m.GetEntityDomain(),
		Name:   m.GetEntity(),
	}

	f.Attribute = &origins.Ident{
		Domain: m.GetAttributeDomain(),
		Name:   m.GetAttribute(),
	}

	f.Value = &origins.Ident{
		Domain: m.GetValueDomain(),
		Name:   m.GetValue(),
	}

	f.Time = chrono.MicroTime(m.GetTime())

	if m.GetAdded() {
		f.Operation = origins.Assertion
	} else {
		f.Operation = origins.Retraction
	}

	return nil
}

// BlockEncoder encodes facts into a buffer.
type BlockEncoder struct {
	Count int

	// Shared buffer for encoding the fact prefix.
	prefix []byte

	// Shared protobuf type for encoding the fact.
	proto *ProtoFact

	// Buffer of bytes containing the encoded facts.
	block *bytes.Buffer
}

// Write encodes a fact and writes to the block.
func (e *BlockEncoder) Write(f *origins.Fact) error {
	data, err := marshalFact(e.proto, f)

	if err != nil {
		return err
	}

	// Size in bytes of the fact.
	size := uint64(len(data))

	if size > maxFactSize {
		err = errors.New(fmt.Sprintf("fact: size %d exceeds maximum allowed %d", size, maxFactSize))
		return err
	}

	// Encode the size of the fact in the prefix.
	binary.PutUvarint(e.prefix, size)

	// Write the data to the buffer..
	e.block.Write(e.prefix)
	e.block.Write(data)

	e.Count++

	return nil
}

// Bytes returns the unread slice of encoded bytes.
func (e *BlockEncoder) Bytes() []byte {
	return e.block.Bytes()
}

// Reset resets the internal buffer and sets the count to zero.
func (e *BlockEncoder) Reset() {
	e.Count = 0
	e.block.Reset()
}

func NewBlockEncoder() *BlockEncoder {
	return &BlockEncoder{
		prefix: make([]byte, factPrefixSize, factPrefixSize),
		block:  bytes.NewBuffer(nil),
		proto:  new(ProtoFact),
	}
}

// BlockDecoder decodes bytes into facts.
type BlockDecoder struct {
	Domain      string
	Transaction uint64
	Count       int

	// Shared buffer for encoding the fact prefix and data.
	prefix []byte
	data   []byte

	// Shared protobuf type for encoding the fact.
	proto *ProtoFact

	// Buffer of bytes containing the encoded facts.
	block *bytes.Buffer

	err error
}

func (d *BlockDecoder) alloc(n int) []byte {
	// Allocate larger buffer.
	if len(d.data) < n {
		d.data = make([]byte, n, n)
	}

	return d.data[:n]
}

func (d *BlockDecoder) Empty() bool {
	return d.block.Len() == 0
}

func (d *BlockDecoder) Next() *origins.Fact {
	if d.err != nil {
		return nil
	}

	var (
		n   int
		err error
	)

	// Read the prefix.
	if n, err = d.block.Read(d.prefix); err != nil {
		d.err = err
		return nil
	}

	// No more bytes.
	if n == 0 {
		return nil
	}

	// Ensure the full prefix was read.
	if n != factPrefixSize {
		d.err = io.ErrUnexpectedEOF
		return nil
	}

	var (
		size    uint64
		errcode int
	)

	// Decode the size of the data.
	if size, errcode = binary.Uvarint(d.prefix); errcode <= 0 {
		logrus.Panic("decode: could not decode fact prefix")
	}

	// Allocate a buffer of the specified size.
	buf := d.alloc(int(size))

	// Read the next bytes for the fact itself.
	if n, err = d.block.Read(buf); err != nil {
		d.err = err
		return nil
	}

	// Ensure the expected number of bytes were read.
	if uint64(n) != size {
		d.err = io.ErrUnexpectedEOF
		return nil
	}

	// Decode the data into the fact.
	fact := &origins.Fact{}

	if err = unmarshalFact(d.proto, buf, d.Domain, d.Transaction, fact); err != nil {
		d.err = err
		return nil
	}

	return fact
}

func (d *BlockDecoder) Err() error {
	if d.err == io.EOF {
		return nil
	}

	return d.err
}

func NewBlockDecoder(block []byte, domain string, tx uint64) *BlockDecoder {
	return &BlockDecoder{
		Domain:      domain,
		Transaction: tx,
		prefix:      make([]byte, factPrefixSize, factPrefixSize),
		block:       bytes.NewBuffer(block),
		proto:       new(ProtoFact),
	}
}
