package run

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/mesh"
)

func TestSharedOwnershipTransactions(t *testing.T) {
	if !durable.Supported {
		t.Skip("requires local file locks")
	}
	const subject = "owner"
	id := runIdentity{Runner: "runner", Subject: subject, ID: "run"}
	path := filepath.Join(t.TempDir(), "run")
	now := time.Now()
	var offset atomic.Int64
	var offline atomic.Bool
	clock := func() (time.Time, error) {
		if offline.Load() {
			return time.Time{}, errors.New("authority unreachable")
		}
		return now.Add(time.Duration(offset.Load())), nil
	}
	duration := 5 * time.Second
	req := contract.StartRequest{ID: id.ID, Model: "chat", Takeover: true, Recoverable: true}
	digest, _ := durable.Digest(req)
	budget := limits{Timeout: time.Hour, ModelRequests: 2, ModelInput: 8192, Output: 8192}
	c := checkpoint{
		Format: 1, Identity: id, Request: req, RequestDigest: digest, BindingVersion: "v1",
		Deadline: now.Add(time.Hour), ReconcileTimeout: time.Second,
		Budget: budget, Balance: budget, Step: 1, ModelInFlight: true,
		Messages: []contract.Message{{User: &req.Input}},
		Owner:    &ownership{Instance: "old", Epoch: 1, Revision: 1, Duration: duration, Expires: now.Add(duration)},
	}
	if _, err := durable.Write(path, c); err != nil {
		t.Fatal(err)
	}
	old := &lease{path: path, id: id, owner: *c.Owner, clock: clock}
	candidate := func(instance, version string) *Runner {
		return &Runner{
			instance: instance, leaseClock: clock,
			recovery: &contract.RecoveryConfig{BindingVersion: version, Takeover: &contract.TakeoverConfig{LeaseDuration: duration}},
			allowed:  map[contract.SubjectID]map[contract.ModelRef]struct{}{subject: {"chat": {}}},
		}
	}
	if _, _, err := candidate("new", "v1").acquireTakeover(t.Context(), path, id); !errors.Is(err, contract.ErrRecoveryBusy) {
		t.Fatalf("active lease: %v", err)
	}
	offset.Store(int64(duration))
	if err := old.renew(t.Context()); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
		t.Fatalf("late renewal: %v", err)
	}
	offset.Store(0)
	if err := old.renew(t.Context()); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
		t.Fatalf("observed expiry was reversed by clock rollback: %v", err)
	}
	offset.Store(int64(duration))
	modelRequest := mesh.MeshRequest{Operation: mesh.Generate, Model: contract.ModelRequest{RunID: id.ID, Step: 1, Model: req.Model},
		Environment: mesh.EnvironmentRequest{Run: contract.RunScope{Subject: id.Subject}}}
	if err := old.Admit(t.Context(), modelRequest); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
		t.Fatalf("expired owner admitted without successor: %v", err)
	}
	if _, _, err := candidate("new", "v2").acquireTakeover(t.Context(), path, id); !errors.Is(err, contract.ErrRecoveryBinding) {
		t.Fatalf("wrong binding: %v", err)
	}
	wrong := id
	wrong.Subject = "other"
	if _, _, err := candidate("new", "v1").acquireTakeover(t.Context(), path, wrong); !errors.Is(err, contract.ErrRecoveryStorage) {
		t.Fatalf("wrong subject: %v", err)
	}
	var wg sync.WaitGroup
	leases := make(chan *lease, 2)
	for _, actor := range []string{"a", "b"} {
		wg.Add(1)
		go func(instance string) {
			defer wg.Done()
			snapshot, owner, err := candidate(instance, "v1").acquireTakeover(t.Context(), path, id)
			if err != nil && !errors.Is(err, contract.ErrRecoveryBusy) {
				t.Errorf("candidate %s: %v", instance, err)
			}
			if err == nil {
				if snapshot.Owner.Epoch != 2 || !snapshot.Deadline.Equal(c.Deadline) {
					t.Errorf("snapshot: %+v", snapshot)
				}
				leases <- owner
			}
		}(actor)
	}
	wg.Wait()
	close(leases)
	if len(leases) != 1 {
		t.Fatalf("takeover winners = %d", len(leases))
	}
	winner := <-leases
	latest, err := readCheckpoint(path, id)
	if err != nil {
		t.Fatal(err)
	}
	stale := latest // Knowledge of the current revision cannot transfer ownership.
	stale.StopOutcome = contract.Cancelled
	stale.Stop = &contract.StopDetail{Code: contract.StopCancelled}
	if err := old.save(&stale); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
		t.Fatalf("old checkpoint with current revision: %v", err)
	}
	cached := latest
	cachedOwner := *cached.Owner
	cachedOwner.Revision--
	cached.Owner = &cachedOwner
	if err := winner.save(&cached); !errors.Is(err, contract.ErrRecoveryRevision) {
		t.Fatalf("current owner with stale revision: %v", err)
	}
	offline.Store(true)
	if err := winner.save(&latest); !errors.Is(err, contract.ErrRecoveryStorage) {
		t.Fatalf("offline commit: %v", err)
	}
	if err := winner.Admit(t.Context(), modelRequest); !errors.Is(err, contract.ErrRecoveryStorage) {
		t.Fatalf("offline dispatch: %v", err)
	}
	called := false
	if err := winner.Publish(t.Context(), contract.RunScope{}, contract.ReceiptKey{}, func() error { called = true; return nil }); !errors.Is(err, contract.ErrRecoveryStorage) || called {
		t.Fatalf("offline publish: %v, touched=%v", err, called)
	}
	offline.Store(false)
	after, err := readCheckpoint(path, id)
	if err != nil || after.Owner.Revision != latest.Owner.Revision || after.Stop != nil || after.ModelAdmitted {
		t.Fatalf("rejected transactions mutated state: %+v, %v", after, err)
	}
	// A previously accepted stop survives another handoff.
	after.StopOutcome, after.Stop = contract.Cancelled, &contract.StopDetail{Code: contract.StopCancelled}
	if err := winner.save(&after); err != nil {
		t.Fatal(err)
	}
	offset.Store(int64(duration * 2))
	stopped, _, err := candidate("third", "v1").acquireTakeover(t.Context(), path, id)
	if err != nil || stopped.Owner.Epoch != 3 || stopped.Stop == nil || stopped.Stop.Code != contract.StopCancelled {
		t.Fatalf("accepted cancellation was lost: %+v (%v)", stopped.Stop, err)
	}
	// The interrupted model intent belongs to the old generation.
	if err := old.Admit(context.Background(), modelRequest); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
		t.Fatalf("delayed model dispatch: %v", err)
	}
}
