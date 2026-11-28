package durable

import (
	"context"
	"errors"
	"os"
	"time"
)

// LockContext waits for a process lock until the caller's context expires.
func LockContext(ctx context.Context, path string) (*os.File, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lock, err := Lock(path)
		if !errors.Is(err, ErrLocked) {
			return lock, err
		}
		timer := time.NewTimer(2 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
