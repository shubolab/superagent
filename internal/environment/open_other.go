//go:build !darwin && !linux

package environment

import (
	"os"

	"github.com/shubolab/superagent/internal/contract"
)

const supportsRootedRead = false

func openNonblocking(*os.Root, string) (*os.File, error) {
	return nil, contract.ErrAccessDenied
}
