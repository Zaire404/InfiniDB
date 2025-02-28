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
	ErrMagic          = errors.New("Magic error")
	ErrTableExists    = errors.New("Table exists")
	ErrTableNotExists = errors.New("Table not exists")
	ErrInvalidOp      = errors.New("Invalid operation")
	ErrFillTables     = errors.New("Fill tables error")
)
