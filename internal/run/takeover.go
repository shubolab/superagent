package run

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/mesh"
)

type ownership struct {
	Instance string
	Epoch    uint64
	Expires  time.Time
	Duration time.Duration
	Revision uint64
	Expired  bool `json:",omitempty"`
}

func (o ownership) matches(other ownership) bool {
	return o.Epoch == other.Epoch && o.Instance == other.Instance
}

type lease struct {
	path  string
	id    runIdentity
	owner ownership
	clock func() (time.Time, error)
}

func (r *Runner) SetTakeoverClock(clock func() (time.Time, error)) { r.leaseClock = clock }

func lockTransaction(ctx context.Context, path string) (*os.File, error) {
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := durable.Lock(path)
		if !errors.Is(err, durable.ErrLocked) {
			return f, err
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

// transact gives checkpoint writes, admission, publication and takeover one order.
func (l *lease) transact(ctx context.Context, action func(*checkpoint, time.Time) error) error {
	lock, err := lockTransaction(ctx, l.path)
	if err != nil {
		return fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
	}
	defer lock.Close()
	c, err := readCheckpoint(l.path, l.id)
	if err != nil {
		return err
	}
	now, err := l.clock()
	if err != nil {
		return fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return action(&c, now)
}

func (l *lease) owned(ctx context.Context, action func(*checkpoint, time.Time) error) error {
	return l.transact(ctx, func(c *checkpoint, now time.Time) error {
		if c.Owner == nil || c.Result != nil || !c.Owner.matches(l.owner) || c.Owner.Expired {
			return contract.ErrRecoveryOwnershipLost
		}
		if !now.Before(c.Owner.Expires) {
			c.Owner.Expired = true
			if err := l.write(c); err != nil {
				return fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
			}
			return contract.ErrRecoveryOwnershipLost
		}
		return action(c, now)
	})
}

func (l *lease) write(c *checkpoint) error {
	_, err := durable.Write(l.path, c)
	return err
}

func (r *Runner) acquireTakeover(ctx context.Context, path string, identity runIdentity) (checkpoint, *lease, error) {
	l := &lease{path: path, id: identity, clock: r.leaseClock}
	var result checkpoint
	err := l.transact(ctx, func(c *checkpoint, now time.Time) error {
		if !c.Request.Takeover || c.Owner == nil {
			return contract.ErrRecoveryUnavailable
		}
		if c.Result != nil {
			result = *c
			return nil
		}
		if c.BindingVersion != r.recovery.BindingVersion {
			return contract.ErrRecoveryBinding
		}
		if err := r.validateRecoveryBindings(*c); err != nil {
			return err
		}
		if err := r.validateRecovery(identity.Subject, c.Request); err != nil {
			return err
		}
		if !c.Owner.Expired && now.Before(c.Owner.Expires) {
			return contract.ErrRecoveryBusy
		}
		if c.Owner.Epoch == ^uint64(0) || c.Owner.Revision == ^uint64(0) {
			return contract.ErrRecoveryStorage
		}
		c.Owner.Instance = r.instance
		c.Owner.Epoch++
		c.Owner.Revision++
		c.Owner.Expires = now.Add(c.Owner.Duration)
		c.Owner.Expired = false
		if err := l.write(c); err != nil {
			return fmt.Errorf("%w: %v", contract.ErrRecoveryStorage, err)
		}
		l.owner, result = *c.Owner, *c
		return nil
	})
	return result, l, err
}

func (l *lease) renew(ctx context.Context) error {
	return l.owned(ctx, func(c *checkpoint, now time.Time) error {
		c.Owner.Expires = now.Add(c.Owner.Duration)
		return l.write(c)
	})
}

func (l *lease) save(next *checkpoint) error {
	return l.owned(context.Background(), func(current *checkpoint, _ time.Time) error {
		if next.Owner == nil || !next.Owner.matches(l.owner) || next.Identity != current.Identity ||
			next.Owner.Revision != current.Owner.Revision || current.Owner.Revision == ^uint64(0) {
			return contract.ErrRecoveryRevision
		}
		owner := *current.Owner
		owner.Revision++
		next.Owner = &owner
		// Admissions have their own idempotency markers. Preserve any admission made
		// while this owner was holding an in-memory checkpoint snapshot.
		if next.ModelInFlight && current.ModelInFlight && next.Step == current.Step {
			next.ModelAdmitted = current.ModelAdmitted
		}
		if next.Intent != nil && current.Intent != nil && next.Intent.Key == current.Intent.Key {
			next.Intent.Admitted = current.Intent.Admitted
		}
		return l.write(next)
	})
}

func (s *state) attachLease(l *lease) {
	s.checkpoint.lease = l
	s.ctx = mesh.WithTakeoverGuard(s.ctx, l)
	go func() {
		ticker := time.NewTicker(max(l.owner.Duration/3, time.Nanosecond))
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				s.mu.Lock()
				if s.checkpointErr != nil || !s.finishedAt.IsZero() {
					s.mu.Unlock()
					return
				}
				if err := l.renew(context.Background()); err != nil {
					s.failCheckpointLocked(err)
					s.mu.Unlock()
					return
				}
				s.mu.Unlock()
			}
		}
	}()
}

func (l *lease) Matches(scope contract.RunScope) bool {
	return scope.Runner == l.id.Runner && scope.Subject == l.id.Subject &&
		scope.RunID == l.id.ID && scope.Epoch == l.owner.Epoch
}

func (l *lease) RemoteOwner() contract.RemoteOwner {
	return contract.RemoteOwner{Instance: l.owner.Instance, Epoch: l.owner.Epoch}
}

func (l *lease) Admit(ctx context.Context, req mesh.MeshRequest) error {
	return l.admit(ctx, req, false)
}

func (l *lease) admit(ctx context.Context, req mesh.MeshRequest, receiver bool) error {
	return l.owned(ctx, func(c *checkpoint, _ time.Time) error {
		if c.Stop != nil || !time.Now().Before(c.Deadline) {
			return contract.ErrRecoveryOwnershipLost
		}
		if req.Operation == mesh.Generate {
			if req.Environment.Run.Subject != l.id.Subject || req.Model.RunID != l.id.ID || req.Model.Model != c.Request.Model ||
				req.Model.Step != c.Step || !c.ModelInFlight || c.ModelAdmitted {
				return contract.ErrAccessDenied
			}
			c.ModelAdmitted = true
			return l.write(c)
		}
		e := req.Environment
		want := valueOr(c.Request.Environment, contract.EnvironmentRequirement{})
		if !l.Matches(e.Run) || e.Ref != want.Ref || e.Run.Workspace != want.Workspace || !e.Run.Deadline.Equal(c.Deadline) {
			return contract.ErrAccessDenied
		}
		if req.Operation == mesh.QueryReceipt {
			if c.Intent == nil || c.Intent.Key != req.ReceiptKey ||
				(!c.Intent.Admitted && (receiver || req.ReceiptKey.Remote == (contract.RemoteToolIdentity{}))) {
				return contract.ErrAccessDenied
			}
			return nil
		}
		if req.Operation == mesh.AllocateEnvironment {
			return nil
		}
		if c.Intent == nil || c.Intent.Key.AdmissionEpoch != l.owner.Epoch ||
			c.Intent.Key.CallID != e.Call.CallID || c.Intent.Key.Tool != e.Call.Tool ||
			c.Intent.Path != e.Call.Path || c.Intent.Limits != e.Call.Limits || e.Call.Run != e.Run {
			return contract.ErrAccessDenied
		}
		switch req.Operation {
		case mesh.GrantEnvironment, mesh.BeginArtifactOutput:
			return nil
		case mesh.OpenEnvironmentRead:
			if !c.Intent.Admitted {
				return contract.ErrAccessDenied
			}
			return nil
		case mesh.Invoke:
			if c.Intent.Admitted || req.ReceiptKey != c.Intent.Key || req.Tool.Limits != c.Intent.Limits ||
				req.ReceiptKey.Fingerprint != durable.ToolFingerprint(req.Tool, e.Call.Path) {
				return contract.ErrAccessDenied
			}
			if req.ReceiptKey.Remote != (contract.RemoteToolIdentity{}) && !receiver {
				return nil
			}
			c.Intent.Admitted = true
			return l.write(c)
		default:
			return contract.ErrAccessDenied
		}
	})
}

func (l *lease) Publish(ctx context.Context, scope contract.RunScope, key contract.ReceiptKey, publish func() error) error {
	return l.owned(ctx, func(c *checkpoint, _ time.Time) error {
		if !l.Matches(scope) || c.Stop != nil || !time.Now().Before(c.Deadline) ||
			c.Intent == nil || !c.Intent.Admitted || c.Intent.Key != key || key.AdmissionEpoch != l.owner.Epoch {
			return contract.ErrAccessDenied
		}
		return publish()
	})
}

// RecoverPublication is scoped to finalizing existing local output for an admitted call.
func (l *lease) RecoverPublication(ctx context.Context, scope contract.RunScope, key contract.ReceiptKey, publish func() error) error {
	return l.owned(ctx, func(c *checkpoint, _ time.Time) error {
		if !l.Matches(scope) || !scope.Deadline.Equal(c.Deadline) || c.Stop != nil || !time.Now().Before(c.Deadline) ||
			c.ArtifactStoreID == "" || key.Remote != (contract.RemoteToolIdentity{}) || key.AdmissionEpoch > l.owner.Epoch ||
			c.Intent == nil || !c.Intent.Admitted || c.Intent.Key != key {
			return contract.ErrAccessDenied
		}
		return publish()
	})
}
