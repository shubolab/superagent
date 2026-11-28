package run

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/schedule"
)

func TestScheduledUnknownWaitsForCheckpointAuthority(t *testing.T) {
	if !durable.Supported {
		t.Skip("local recovery is unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	pool, err := schedule.New(contract.SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: 1, MaxQueueWait: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{ref: "main", recovery: &contract.RecoveryConfig{Directory: t.TempDir()}, scheduler: pool}
	path, identity := r.recoveryPath("owner", "A")
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := pool.Admit(ctx, schedule.Session{Subject: "owner", ID: "A"}, "A", "A", func(ctx context.Context) error {
		r.reconcileScheduledStart(pool.Permit(ctx, "owner", "A"), path, identity, "original")
		return contract.ErrStartOutcomeUnknown
	}); !errors.Is(err, contract.ErrStartOutcomeUnknown) {
		t.Fatalf("initial response: %v", err)
	}
	admitted := make(chan error, 1)
	go func() {
		admitted <- pool.Admit(ctx, schedule.Session{Subject: "owner", ID: "B"}, "B", "B", func(context.Context) error { return nil })
	}()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if active, queued := pool.Counts(); active == 1 && queued == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-admitted:
		if err != nil {
			t.Fatalf("trusted idle confirmation did not advance B: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if active, queued := pool.Counts(); active != 0 || queued != 0 {
		t.Fatalf("retained capacity: %d/%d", active, queued)
	}
}

func TestScheduledCleanupPendingIsTerminal(t *testing.T) {
	pool, err := schedule.New(contract.SessionSchedulingConfig{MaxActiveRuns: 1})
	if err != nil {
		t.Fatal(err)
	}
	s := newState(contract.StartRequest{ID: "A", Tools: []contract.ToolRef{"tool"}}, limits{}, time.Now().Add(time.Minute), 1024)
	if err := pool.Admit(t.Context(), schedule.Session{Subject: "owner", ID: "A"}, "A", "A", func(ctx context.Context) error {
		s.attachPermit(pool.Permit(ctx, "owner", "A"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	s.finishLocked(contract.Failed, nil, nil, &contract.StopDetail{Code: contract.StopEnvironmentFailure})
	if active, _ := pool.Counts(); active != 1 {
		t.Fatal("released before authoritative cleanup state")
	}
	s.publishFinishedLocked(contract.CleanupPending)
	if active, queued := pool.Counts(); active != 0 || queued != 0 || s.result.Cleanup != contract.CleanupPending {
		t.Fatalf("terminal cleanup retained Run capacity: %d/%d, result %+v", active, queued, s.result)
	}
}

func TestScheduledRecoveryRetainsPermitWhenReplacingFencedWorker(t *testing.T) {
	if !durable.Supported {
		t.Skip("local recovery is unavailable")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	pool, err := schedule.New(contract.SessionSchedulingConfig{MaxActiveRuns: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	dispatch, finish := make(chan struct{}), make(chan struct{})
	r := &Runner{
		ref: "main", instance: "local", scheduler: pool, retention: time.Minute,
		observeBufferBytes: 4096, runs: make(map[runKey]*state),
		allowed: map[contract.SubjectID]map[contract.ModelRef]struct{}{"owner": {"chat": {}}},
		recovery: &contract.RecoveryConfig{Directory: t.TempDir(), BindingVersion: "v1",
			Takeover: &contract.TakeoverConfig{LeaseDuration: time.Minute}},
		leaseClock: func() (time.Time, error) { return now, nil },
		open: func(ctx context.Context, _ contract.SubjectID, _ contract.ModelRequest) (contract.ModelStream, error) {
			close(dispatch)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-finish:
				return nil, errors.New("controlled model failure")
			}
		},
	}
	req := contract.StartRequest{ID: "A", Model: "chat", Recoverable: true, Takeover: true}
	digest, err := durable.Digest(req)
	if err != nil {
		t.Fatal(err)
	}
	path, identity := r.recoveryPath("owner", req.ID)
	budget := limits{Timeout: time.Minute, ModelRequests: 1, ModelInput: 4096, Output: 4096}
	owner := ownership{Instance: r.instance, Epoch: 1, Revision: 1, Duration: time.Minute, Expires: now}
	checkpoint := checkpoint{
		Format: 1, Identity: identity, Request: req, RequestDigest: digest, BindingVersion: "v1",
		Deadline: now.Add(time.Minute), ReconcileTimeout: time.Second, Budget: budget, Balance: budget, Step: 1,
		Owner: &owner, Messages: []contract.Message{{User: &req.Input}},
	}
	if _, err := durable.Write(path, checkpoint); err != nil {
		t.Fatal(err)
	}
	old := newState(req, budget, checkpoint.Deadline, r.observeBufferBytes)
	old.checkpoint = &runCheckpoint{path: path, data: checkpoint,
		lease: &lease{path: path, id: identity, owner: owner, clock: r.leaseClock}}
	r.runs[runKey{subject: identity.Subject, id: identity.ID}] = old
	session := schedule.Session{Subject: "owner", ID: "A"}
	if err := pool.Admit(ctx, session, "A", digest, func(ctx context.Context) error {
		old.attachPermit(pool.Permit(ctx, "owner", "A"))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := pool.Admit(ctx, session, "A", digest, func(ctx context.Context) error {
		_, err := r.Recover(ctx, "owner", "A")
		return err
	}); err != nil {
		t.Fatalf("recovery lost a valid permit with the fenced worker: %v", err)
	}
	select {
	case <-dispatch:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if active, queued := pool.Counts(); active != 1 || queued != 0 {
		t.Fatalf("replacement Run count: %d/%d", active, queued)
	}
	close(finish)
	if result, err := r.Wait(ctx, "owner", "A"); err != nil || result.Outcome != contract.Failed {
		t.Fatalf("replacement terminal: %s, %v", result.Outcome, err)
	}
	if active, _ := pool.Counts(); active != 0 {
		t.Fatalf("replacement terminal left %d permits", active)
	}
}
