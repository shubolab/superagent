package run

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

// SessionBinding identifies the retained local checkpoint store and assembly.
func (r *Runner) SessionBinding(ctx context.Context) (string, error) {
	if r.recovery == nil {
		return "", contract.ErrRecoveryUnavailable
	}
	dir := filepath.Clean(r.recovery.Directory)
	path := filepath.Join(dir, ".session-authority")
	lock, err := durable.LockContext(ctx, path)
	if err != nil {
		return "", fmt.Errorf("%w: %w", contract.ErrRecoveryStorage, err)
	}
	defer lock.Close()
	var id string
	if err := durable.Read(path, &id); errors.Is(err, os.ErrNotExist) {
		id = rand.Text()
		if _, err := durable.Write(path, id); err != nil {
			return "", fmt.Errorf("%w: %w", contract.ErrRecoveryStorage, err)
		}
	} else if err != nil || id == "" {
		return "", contract.ErrRecoveryStorage
	}
	if err := durable.SyncDirectory(dir); err != nil {
		return "", fmt.Errorf("%w: %w", contract.ErrRecoveryStorage, err)
	}
	return durable.Digest([3]string{dir, id, r.recovery.BindingVersion})
}

// SessionRequestDigest uses the same request normalization as durable admission.
func (r *Runner) SessionRequestDigest(req contract.StartRequest) (string, error) {
	req.Environment = r.normalizedEnvironment(req.Environment)
	return durable.Digest(freezeStart(req))
}

// SessionCheckpoint distinguishes absent, compatible, expired and invalid records.
// A missing file alone is insufficient to retry an attempted admission.
func (r *Runner) SessionCheckpoint(subject contract.SubjectID, id contract.RunID, digest string) (bool, error) {
	if r.recovery == nil {
		return false, contract.ErrRecoveryUnavailable
	}
	path, identity := r.recoveryPath(subject, id)
	record, err := readCheckpoint(path, identity)
	if errors.Is(err, contract.ErrRunNotFound) {
		r.mu.Lock()
		_, running := r.runs[runKey{subject: subject, id: id}]
		r.mu.Unlock()
		if running {
			return false, contract.ErrStartConflict
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if record.RequestDigest != digest {
		return false, contract.ErrStartConflict
	}
	if record.BindingVersion != r.recovery.BindingVersion {
		return false, contract.ErrRecoveryBinding
	}
	if record.Result != nil && !time.Now().Before(record.FinishedAt.Add(r.retention)) {
		return false, contract.ErrRunNotFound
	}
	return true, nil
}
