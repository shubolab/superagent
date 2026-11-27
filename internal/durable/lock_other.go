//go:build !darwin && !linux

package durable

import (
	"errors"
	"os"
)

const Supported = false

func Lock(string) (*os.File, error) {
	return nil, errors.New("local recovery locking is unavailable")
}
