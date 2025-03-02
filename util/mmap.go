package util

import (
	"os"

	"golang.org/x/sys/unix"
)

// Mmap maps a file into memory.
func Mmap(fd *os.File, size int, writable bool) ([]byte, error) {
	var mode int
	if writable {
		mode = unix.PROT_READ | unix.PROT_WRITE
	} else {
		mode = unix.PROT_READ
	}
	return unix.Mmap(int(fd.Fd()), 0, size, mode, unix.MAP_SHARED)
}

// Msync flushes changes made to the in-core copy of a file that was mapped into memory using Mmap.
func Msync(data []byte) error {
	return unix.Msync(data, unix.MS_SYNC)
}

// Munmap unmaps a file from memory.
func Munmap(data []byte) error {
	return unix.Munmap(data)
}

// Mremap unmmap and mmap
func Mremap(data []byte, size int) ([]byte, error) {
	return unix.Mremap(data, size, unix.MREMAP_MAYMOVE)
}
