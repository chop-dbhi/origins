package storage

import (
	"encoding/json"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Codec interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
	String() string
}

type JSONCodec struct{}

func (c *JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (c *JSONCodec) Unmarshal(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}

func (c *JSONCodec) String() string {
	return "json"
}

type MessagePackCodec struct{}

func (c *MessagePackCodec) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (c *MessagePackCodec) Unmarshal(b []byte, v interface{}) error {
	return msgpack.Unmarshal(b, v)
}

func (c *MessagePackCodec) String() string {
	return "msgpack"
}

type BSONCodec struct{}

func (c *BSONCodec) Marshal(v interface{}) ([]byte, error) {
	return bson.Marshal(v)
}

func (c *BSONCodec) Unmarshal(b []byte, v interface{}) error {
	return bson.Unmarshal(b, v)
}

func (c *BSONCodec) String() string {
	return "bson.v2"
}

type ProtobufCodec struct{}

func (c *ProtobufCodec) Marshal(v interface{}) ([]byte, error) {
	switch x := v.(type) {
	case ProtoMessage:
		m, err := x.ToProto()

		if err != nil {
			return nil, err
		}

		return proto.Marshal(m)
	default:
		logrus.Panicf("%T does not support protobuf", x)
	}

	return nil, nil
}

func (c *ProtobufCodec) Unmarshal(b []byte, v interface{}) error {
	switch x := v.(type) {
	case ProtoMessage:
		m := x.Proto()

		if err := proto.Unmarshal(b, m); err != nil {
			return err
		}

		return x.FromProto(m)
	default:
		logrus.Panicf("%T does not support protobuf", x)
	}

	return nil
}

func (c *ProtobufCodec) String() string {
	return "protobuf.v2"
}

var codecs = map[string]Codec{
	"json":     &JSONCodec{},
	"bson":     &BSONCodec{},
	"msgpack":  &MessagePackCodec{},
	"protobuf": &ProtobufCodec{},
}
