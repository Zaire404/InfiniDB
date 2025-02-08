package util

import (
	"path"
	"strconv"
	"strings"

	. "github.com/Zaire404/InfiniDB/error"
)

func GetFIDByPath(tablepath string) (uint64, error) {
	name := path.Base(tablepath)
	if !strings.HasSuffix(name, ".sst") {
		return 0, ErrInvalidName
	}
	name = strings.TrimSuffix(name, ".sst")
	fid, err := strconv.Atoi(name)
	if err != nil {
		return 0, err
	}
	return uint64(fid), nil
}

func GenSSTName(fid uint64) string {
	return strconv.Itoa(int(fid)) + ".sst"
}

func VerifyCheckSum(data []byte, checksum []byte) bool {
	actual := Checksum(data)
	expected := BytesToUint32(checksum)
	return actual == expected
}

func DiffKey(baseKey []byte, newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(baseKey); i++ {
		if newKey[i] != baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

func RecoverKey(baseKey []byte, diffKey []byte, overlap uint16) []byte {
	return append(baseKey[:overlap], diffKey...)
}
