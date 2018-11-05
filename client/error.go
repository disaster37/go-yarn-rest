package client

import (
	"fmt"
)

type YarnError struct {
	Code    int
	Message string
}

func (e YarnError) Error() string {
	return e.Message
}

func NewYarnError(code int, message string, params ...interface{}) YarnError {
	return YarnError{
		Code:    code,
		Message: fmt.Sprintf(message, params...),
	}
}
