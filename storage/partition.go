package storage

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

const (
	// Takes the store name and partition (domain) name.
	partitionKeyFmt = "%s.%s"

	// Takes the partition key and the segment key.
	segmentKeyFmt = "%s.%d"
)

// segment is a chunk of bytes in a partition.
type segment struct {
	key      uint64
	data     []byte
	storeKey string
}

// partition corresponds to a topic of data in a domain. A partition is
// chunked up into one or more segments of data. The parti
type partition struct {
	// SegmentKeys is an ordered slice of segment keys.
	SegmentKeys []uint64

	// segments is a local cache of segments that have created or loaded
	// in this session.
	// TODO(bjr): should there be a max cache size?
	segments map[uint64]*segment

	// storeKey is the key provided by the store for this partition.
	storeKey string

	// engine is a reference to the storage engine the store uses.
	engine Engine

	// temp is a partition used strictly to encode data for the store.
	temp *partition

	// mux is a mutex for updating the internal cache and writing to the store.
	mux sync.Mutex
}

// Proto satisfies the storage.ProtoMessage interface.
func (p *partition) Proto() proto.Message {
	return &ProtoPartition{}
}

// ToProto satisfies the storage.ProtoMessage interface.
func (p *partition) ToProto() (proto.Message, error) {
	return &ProtoPartition{
		SegmentKeys: p.SegmentKeys,
	}, nil
}

// FromProto satisfies the storage.ProtoMessage interface.
func (p *partition) FromProto(m proto.Message) error {
	t := m.(*ProtoPartition)
	p.SegmentKeys = t.GetSegmentKeys()
	return nil
}

// get returns a segment by key, getting it from storage if necessary.
func (p *partition) get(key uint64) *segment {
	s, ok := p.segments[key]

	if ok {
		return s
	}

	skey := fmt.Sprintf(segmentKeyFmt, p.storeKey, key)
	data, err := p.engine.Get(skey)

	if err != nil {
		logrus.Panicf("failed to load segment %s: %s", skey, err)
	}

	if data == nil {
		return nil
	}

	logrus.Debugf("loaded segment %d from storage", key)

	s = &segment{
		key:  key,
		data: data,
	}

	p.segments[key] = s

	return s

}

// Write writes a segment to the partition.
func (p *partition) Write(key uint64, data []byte) error {
	p.mux.Lock()
	defer p.mux.Unlock()

	var (
		s   *segment
		err error
	)

	if s = p.get(key); s != nil {
		err = errors.New(fmt.Sprintf("segment key %s.%d already exists!", p.storeKey, key))
		logrus.Error(err)
		return err
	}

	storeKey := fmt.Sprintf(segmentKeyFmt, p.storeKey, key)

	s = &segment{
		key:      key,
		data:     data,
		storeKey: storeKey,
	}

	// Add to temp partition for writing to the store.
	p.temp.SegmentKeys = append(p.SegmentKeys, s.key)

	header, err := headerCodec.Marshal(p.temp)

	if err != nil {
		logrus.Panic(err)
	}

	// Batch the new key and header state.
	batch := Batch{
		s.storeKey: s.data,
		p.storeKey: header,
	}

	if err := p.engine.SetMany(batch); err != nil {
		return err
	}

	// Add segment to local cache after the write succeeded
	p.segments[s.key] = s
	p.SegmentKeys = p.temp.SegmentKeys
	p.temp.SegmentKeys = nil

	return nil
}

// Reader returns a partition reader which provide isolated control over
// reading the segments in the partition.
func (p *partition) Reader() *partitionReader {
	return &partitionReader{
		p: p,
	}
}

type partitionReader struct {
	// Pointer to the partition for access to the header and engine.
	p *partition
	// Current index in the header keys. This corresponds to a segment key.
	index int
	// Current byte position in the segment.
	pos int
}

// Read satisfies the io.Reader interface.
func (r *partitionReader) Read(buf []byte) (int, error) {
	// Empty partition
	if len(r.p.SegmentKeys) == 0 {
		logrus.Debugf("partition is empty")
		return 0, io.EOF
	}

	// Key to a segment.
	key := r.p.SegmentKeys[r.index]

	// Get the segment and the length of the data slice.
	seg := r.p.get(key)
	slen := len(seg.data)

	// Length of the buffer
	blen := len(buf)

	// Offset of the current byte being read.
	i := 0

	for {
		// buf is filled
		if i == blen {
			break
		}

		// check if the segment as run out, load the next one
		if r.pos >= slen {
			r.index += 1
			r.pos = 0

			// No more segments available
			if r.index+1 > len(r.p.SegmentKeys) {
				return i, io.EOF
			}

			key = r.p.SegmentKeys[r.index]
			seg = r.p.get(key)
			slen = len(seg.data)
		}

		// Copy into buffer.
		buf[i] = seg.data[r.pos]

		// Increment the offsets.
		i += 1
		r.pos += 1
	}

	return i, nil
}

// initPartition initializes a partition for the key. If the partition does
// not exist in the store, it will be created by writing an empty header.
func initPartition(key string, engine Engine) (*partition, error) {
	var (
		b   []byte
		err error
	)

	b, err = engine.Get(key)

	if err != nil {
		return nil, err
	}

	p := partition{
		SegmentKeys: make([]uint64, 0),
		engine:      engine,
		storeKey:    key,
		segments:    make(map[uint64]*segment),
		temp:        &partition{},
	}

	// Partition already exists, populate the header.
	if b != nil {
		if err = headerCodec.Unmarshal(b, &p); err != nil {
			return nil, err
		}

		logrus.Infof("loaded existing partition: %s", p.storeKey)
	}

	return &p, nil
}
