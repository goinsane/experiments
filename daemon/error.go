package daemon

import (
	"errors"
	"fmt"
)

// Error is type of error
type Error struct {
	err error
}

// newError wraps err into Error
func newError(err error) error {
	if err == nil {
		return nil
	}
	return &Error{
		err: err,
	}
}

// Error is implementation of error
func (e *Error) Error() string {
	s := "daemon error"
	if e.err == nil {
		return s
	}
	return fmt.Sprintf("%s: %v", s, e.err)
}

// Unwrap returns wrapped error
func (e *Error) Unwrap() error {
	return e.err
}

var (
	ErrInvalidJobId           = errors.New("invalid job id")
	ErrJobIdAlreadyRegistered = errors.New("job id already registered")
	ErrJobIdNotRegistered     = errors.New("job id not registered")
	ErrEventAlreadyRegistered = errors.New("event already registered")
	ErrEventNotRegistered     = errors.New("event not registered")
	ErrEventBufferFull        = errors.New("event buffer full")
)
