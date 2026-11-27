//go:build darwin || linux

package environment

import (
	"os"
	"syscall"
)

const supportsRootedRead = true

func openNonblocking(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
