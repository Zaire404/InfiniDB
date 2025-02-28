package util

func SafeCopy(needKey, key []byte) []byte {
	return append(needKey[:0], key...)
}
