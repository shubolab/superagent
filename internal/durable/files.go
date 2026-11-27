// Package durable provides local process-safe storage for checkpoints and receipts.
package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

const MaxRecordBytes = 32 << 20

var ErrLocked = errors.New("durable record is locked")
var errRecordLimit = errors.New("durable record exceeds size limit")

func Directory(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("durable storage requires an absolute directory")
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func Digest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func Read(path string, dst any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, MaxRecordBytes+1))
	if err != nil {
		return err
	}
	if len(data) > MaxRecordBytes {
		return errRecordLimit
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("invalid durable record suffix")
	}
	return nil
}

// Write distinguishes failures after rename, where the commit must be checked by reopening.
func Write(path string, value any) (renamed bool, err error) {
	data, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	if len(data) > MaxRecordBytes {
		return false, errRecordLimit
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".record-")
	if err != nil {
		return false, err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return false, err
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return false, err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return false, err
	}
	return true, syncDirectory(dir)
}

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
