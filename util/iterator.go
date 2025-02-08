package util

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	SeekToFirst()
	SeekToLast()
	Seek(key []byte)
	Item() Item
	Close() error
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	IsAsc bool
}
