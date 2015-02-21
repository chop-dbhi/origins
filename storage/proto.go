package storage

import "github.com/golang/protobuf/proto"

// ProtoMessage defines methods for converting to and from a type's
// protocol buffer message type.
type ProtoMessage interface {
	// Proto returns an initialized value to decode byte into.
	Proto() proto.Message

	// ToProto returns a protobuf message.
	ToProto() (proto.Message, error)

	// FromProto takes a proto message and populates the value with the fields.
	FromProto(proto.Message) error
}
