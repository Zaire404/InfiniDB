package util

import (
	"bufio"
	"os"
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
	fid, err := strconv.ParseUint(name, 10, 64)
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
	newKey := make([]byte, int(overlap)+len(diffKey))
	copy(newKey, baseKey[:overlap])
	copy(newKey[overlap:], diffKey)
	return newKey
}

func CollectIDMap(dir string) (map[uint64]struct{}, error) {
	filesInfo, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	idMap := make(map[uint64]struct{})
	for _, info := range filesInfo {
		if info.IsDir() {
			continue
		}
		fid, err := GetFIDByPath(info.Name())
		if err != nil {
			continue
		}
		if fid != 0 {
			idMap[fid] = struct{}{}
		}
	}
	return idMap, nil
}

type BufReader struct {
	Reader *bufio.Reader
	Offset int64
}

func (r *BufReader) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	r.Offset += int64(n)
	return n, err
}
