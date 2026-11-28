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
	"github.com/shubolab/superagent/internal/schedule"
)

const sessionAuthority = ".session-authority"

// SessionBinding identifies the retained local checkpoint store and assembly.
func (r *Runner) SessionBinding(ctx context.Context) (string, error) {
	if r.recovery == nil {
		return "", contract.ErrRecoveryUnavailable
	}
	dir := filepath.Clean(r.recovery.Directory)
	path := filepath.Join(dir, sessionAuthority)
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

// InspectSessionBinding reads the existing authority without creating or syncing it.
func (r *Runner) InspectSessionBinding(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if r.recovery == nil {
		return "", contract.ErrRecoveryUnavailable
	}
	dir := filepath.Clean(r.recovery.Directory)
	var id string
	if err := durable.Read(filepath.Join(dir, sessionAuthority), &id); err != nil || id == "" {
		return "", contract.ErrRecoveryBinding
	}
	return durable.Digest([3]string{dir, id, r.recovery.BindingVersion})
}

// SessionRequestDigest uses the same request normalization as durable admission.
func (r *Runner) SessionRequestDigest(req contract.StartRequest) (string, error) {
	req.Environment = r.normalizedEnvironment(req.Environment)
	return durable.Digest(freezeStart(req))
}

func (r *Runner) SessionScheduler() *schedule.Scheduler { return r.scheduler }

// SessionCheckpoint distinguishes missing, continuable and terminal records.
// A missing file alone is insufficient to retry an attempted admission.
func (r *Runner) SessionCheckpoint(subject contract.SubjectID, id contract.RunID, digest string) (contract.SessionRunCheckpoint, error) {
	var missing contract.SessionRunCheckpoint
	if r.recovery == nil {
		return missing, contract.ErrRecoveryUnavailable
	}
	path, identity := r.recoveryPath(subject, id)
	record, err := readCheckpoint(path, identity)
	if errors.Is(err, contract.ErrRunNotFound) {
		r.mu.Lock()
		_, running := r.runs[runKey{subject: subject, id: id}]
		if running {
			record, err = readCheckpoint(path, identity)
		}
		r.mu.Unlock()
		if running && errors.Is(err, contract.ErrRunNotFound) {
			return missing, contract.ErrStartConflict
		}
		if !running {
			return missing, nil
		}
	}
	if err != nil {
		return missing, err
	}
	if record.RequestDigest != digest {
		return missing, contract.ErrStartConflict
	}
	if record.BindingVersion != r.recovery.BindingVersion {
		return missing, contract.ErrRecoveryBinding
	}
	if record.Result != nil && !time.Now().Before(record.FinishedAt.Add(r.retention)) {
		return missing, contract.ErrRunNotFound
	}
	return contract.SessionRunCheckpoint{Exists: true, Result: record.Result}, nil
}

func (r *Runner) schedulingPermit(ctx context.Context, subject contract.SubjectID, id contract.RunID) *schedule.Permit {
	if r.scheduler == nil {
		return nil
	}
	return r.scheduler.Permit(ctx, subject, id)
}

// A failed directory sync can leave the initial checkpoint visible without a live Run.
// Reconcile it locally, while a concurrent recovery may take over the same permit.
func (r *Runner) reconcileScheduledStart(permit *schedule.Permit, path string, identity runIdentity, digest string) {
	resolved := permit.MarkUncertain()
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-resolved:
				return
			case <-ticker.C:
			}
			if r.confirmIdleAdmission(permit, path, identity, digest) {
				return
			}
		}
	}()
}

func (r *Runner) confirmIdleAdmission(permit *schedule.Permit, path string, identity runIdentity, digest string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runs[runKey{subject: identity.Subject, id: identity.ID}] != nil {
		return false
	}
	record, err := readCheckpoint(path, identity)
	if !errors.Is(err, contract.ErrRunNotFound) && (err != nil || record.RequestDigest != digest) {
		return false
	}
	if err := durable.SyncDirectory(filepath.Dir(path)); err != nil {
		return false
	}
	return permit.ConfirmStopped()
}
