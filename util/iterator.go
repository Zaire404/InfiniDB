package util

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	SeekToFirst()
	SeekToLast()
	Seek(key []byte) // Seek moves the iterator to the first entry with a key >= target
	Item() Item
	Close() error
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	IsAsc bool
}
