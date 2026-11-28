//go:build darwin || linux

package durable

import (
	"errors"
	"os"
	"syscall"
)

const Supported = true

// Lock uses a separate inode because checkpoint writes replace the data file.
func Lock(path string) (*os.File, error) {
	return lockFile(path, os.O_CREATE|os.O_RDWR, syscall.LOCK_EX)
}

// ReadLock does not create or change the lock file.
func ReadLock(path string) (*os.File, error) {
	return lockFile(path, os.O_RDONLY, syscall.LOCK_SH)
}

func lockFile(path string, flag, operation int) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", flag, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), operation|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return f, nil
}
