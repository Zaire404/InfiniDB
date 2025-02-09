package error

import (
	"errors"
)

var (
	ErrEmptyKey       = errors.New("Key cannot be empty")
	ErrKeyNotFound    = errors.New("Key not found")
	ErrInvalidName    = errors.New("Invalid file name")
	ErrReadOutOfBound = errors.New("Read out of bound")
	ErrChecksum       = errors.New("Checksum error")
)
