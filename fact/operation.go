package fact

import (
	"errors"
	"fmt"
	"strings"
)

type Operation string

const (
	AssertOp  Operation = "assert"
	RetractOp Operation = "retract"
)

func ParseOperation(s string) (Operation, error) {
	switch strings.ToLower(s) {
	case "", "assert":
		return AssertOp, nil
	case "retract":
		return RetractOp, nil
	}

	return "", errors.New(fmt.Sprintf("unknown operation: %s", s))
}
