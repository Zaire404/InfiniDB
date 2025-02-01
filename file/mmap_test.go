package file

import (
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenMmapFileNonEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	filename := filepath.Join(dir, "test_non_empty")
	f, err := os.Create(filename)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	mf, err := OpenMmapFile(filename, os.O_RDONLY, 0)
	require.NoError(t, err)
	defer mf.Close()
	require.Equal(t, []byte("hello"), mf.Data)
}
func TestOpenMmapFileEmpty(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	filename := filepath.Join(dir, "test_empty")
	f, err := os.Create(filename)
	require.NoError(t, err)
	defer f.Close()
	mf, err := OpenMmapFile(filename, os.O_RDWR, 0)
	require.Error(t, err)
	mf, err = OpenMmapFile(filename, os.O_RDWR, 10)
	defer mf.Close()
	require.NoError(t, err)
	require.Equal(t, 10, len(mf.Data))
}

func TestOpenMmapFileNonExistent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	filename := filepath.Join(dir, "non_existent")
	_, err := OpenMmapFile(filename, os.O_RDONLY, 10)
	require.Error(t, err)
	log.Println(err)
}

func TestOpenMmapFileTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	filename := filepath.Join(dir, "test_truncate")
	f, err := os.Create(filename)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, err)
	mf, err := OpenMmapFile(filename, os.O_RDWR, 20)
	require.NoError(t, err)
	defer mf.Close()
	require.Equal(t, 20, len(mf.Data))
}
