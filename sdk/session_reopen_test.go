//go:build darwin || linux

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

func TestInspectPendingReadOnly(t *testing.T) {
	ctx, root := sessionTestContext(t), t.TempDir()
	var dispatched atomic.Int32
	model := deliveryModel(func(context.Context, contract.ModelRequest) (contract.ModelStream, error) {
		dispatched.Add(1)
		return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "saved"}}, nil
	})
	client := sessionTestClient(t, root, "v1", time.Minute, model, nil)
	s := openTestSession(t, ctx, client, root, "A")
	requireState := func(id contract.RunID, want PendingRecoveryState, cause error) {
		t.Helper()
		before := recoveryFiles(t, root)
		state, err := s.InspectPending(ctx)
		if err != nil || state.RunID != id || state.State != want || !errors.Is(state.Cause, cause) {
			t.Fatalf("InspectPending = %+v, %v, want %s, %s, %v", state, err, id, want, cause)
		}
		if !reflect.DeepEqual(before, recoveryFiles(t, root)) {
			t.Fatal("inspection wrote a file")
		}
	}
	requireState("", RecoveryIdle, nil)
	req := sessionRequest(s, "reserved draft")
	req.Recoverable = true
	runner, err := s.runner(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.reserve(ctx, runner, req); err != nil {
		t.Fatal(err)
	}
	requireState(req.ID, RecoveryUnadmitted, nil)

	idx, err := fileSessionStore(s).Load()
	if err != nil {
		t.Fatal(err)
	}
	original := idx.Pending.RequestDigest
	idx.Pending.RequestDigest, err = durable.Digest("different request")
	if err != nil {
		t.Fatal(err)
	}
	if err := fileSessionStore(s).Save(idx); err != nil {
		t.Fatal(err)
	}
	requireState(req.ID, RecoveryUnknown, contract.ErrStartConflict)
	idx.Pending.RequestDigest = original
	if err := fileSessionStore(s).Save(idx); err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(root, "runs", ".session-authority")
	if err := os.Rename(marker, marker+".offline"); err != nil {
		t.Fatal(err)
	}
	requireState(req.ID, RecoveryUnknown, contract.ErrRecoveryBinding)
	if err := os.Rename(marker+".offline", marker); err != nil {
		t.Fatal(err)
	}
	discarded, err := s.DiscardUnadmitted(ctx, req.ID)
	if err != nil || discarded.RunID != req.ID || discarded.State != RecoveryUnadmitted || discarded.Draft != req.Input.Text {
		t.Fatalf("DiscardUnadmitted = %+v, %v", discarded, err)
	}
	requireState("", RecoveryIdle, nil)

	req.ID = s.NewRunID()
	if _, err := s.reserve(ctx, runner, req); err != nil {
		t.Fatal(err)
	}
	state, err := s.store.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.MarkAttempted(ctx, state.Pending.Key()); err != nil {
		t.Fatal(err)
	}
	requireState(req.ID, RecoveryUnknown, contract.ErrRunNotFound)
	if _, err := s.CommitPendingResult(ctx, req.ID); !errors.Is(err, ErrSessionResultUnavailable) {
		t.Fatalf("save without trusted terminal = %v", err)
	}
	update := readStartProgress(t, ctx, s.ResumeAdmittedWithProgress(ctx, req.ID), req.ID, StartResolved)
	if !errors.Is(update.Err, contract.ErrRunNotFound) || update.Run != nil {
		t.Fatalf("resume without a checkpoint = %+v", update)
	}
	if state, err := s.DiscardUnadmitted(ctx, req.ID); err != nil || state.State != RecoveryUnknown {
		t.Fatalf("discarding an attempted reservation = %+v, %v", state, err)
	}
	requireState(req.ID, RecoveryUnknown, contract.ErrRunNotFound)
	if dispatched.Load() != 0 {
		t.Fatal("recovery inspection or rejected action dispatched a model")
	}
}

func recoveryFiles(t *testing.T, root string) map[string]struct {
	Modified time.Time
	Data     string
} {
	t.Helper()
	out := make(map[string]struct {
		Modified time.Time
		Data     string
	})
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err == nil {
			out[path] = struct {
				Modified time.Time
				Data     string
			}{info.ModTime(), string(data)}
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCommitPendingResultIdempotent(t *testing.T) {
	ctx, root := sessionTestContext(t), t.TempDir()
	var calls atomic.Int32
	model := deliveryModel(func(context.Context, contract.ModelRequest) (contract.ModelStream, error) {
		calls.Add(1)
		return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "saved"}}, nil
	})
	client := sessionTestClient(t, root, "v1", time.Minute, model, nil)
	s := openTestSession(t, ctx, client, root, "A")
	req := sessionRequest(s, "first")
	waitSessionRun(t, ctx, startSessionRun(t, ctx, s, req), contract.Succeeded)
	path := pendingTestCheckpoint(t, root, req.ID)
	var checkpoint map[string]json.RawMessage
	if err := durable.Read(path, &checkpoint); err != nil {
		t.Fatal(err)
	}
	terminal := checkpoint["Result"]
	corrupt, err := json.Marshal(contract.Result{RunID: req.ID, Outcome: contract.Succeeded})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint["Result"] = corrupt
	if _, err := durable.Write(path, checkpoint); err != nil {
		t.Fatal(err)
	}
	if p, err := s.InspectPending(ctx); err != nil || p.RunID != req.ID || p.State != RecoveryUnknown || !errors.Is(p.Cause, ErrSessionContent) {
		t.Fatalf("malformed terminal was trusted: %+v, %v", p, err)
	}
	checkpoint["Result"] = terminal
	if _, err := durable.Write(path, checkpoint); err != nil {
		t.Fatal(err)
	}
	p, err := s.InspectPending(ctx)
	if err != nil || p.RunID != req.ID || p.State != RecoverySave {
		t.Fatalf("terminal inspection = %+v, %v", p, err)
	}
	update := readStartProgress(t, ctx, s.ResumeAdmittedWithProgress(ctx, req.ID), req.ID, StartResolved)
	if !errors.Is(update.Err, ErrPendingResultReady) {
		t.Fatalf("terminal offered for further execution: %+v", update)
	}
	first, err := s.CommitPendingResult(ctx, req.ID)
	if err != nil || first.RunID != req.ID || first.Version != 1 {
		t.Fatalf("first receipt = %+v, %v", first, err)
	}
	again, err := s.CommitPendingResult(ctx, req.ID)
	if err != nil || again != first || calls.Load() != 1 {
		t.Fatalf("duplicate save = %+v, %v, model calls %d", again, err, calls.Load())
	}
	assertSessionVersion(t, ctx, s, 1, 2)
}

func TestResumeAdmittedRechecksAfterGrant(t *testing.T) {
	ctx, root := sessionTestContext(t), t.TempDir()
	runSessionChild(t, root, "schedule-source", "schedule-queued")
	id := readRecoveryLog[[]contract.RunID](t, root, "schedule-ids")[0][0]
	gate := make(chan struct{})
	assembly, pool := scheduleRecoveryAssembly(t, root, &scheduleRecoveryModel{root: root, holdGate: gate})
	client := scheduledClient(t, assembly)
	a := openTestSession(t, ctx, client, root, "A")
	holder := openTestSession(t, ctx, client, root, "holder")
	active := startSessionRun(t, ctx, holder, sessionRequest(holder, "holding"))
	updates := a.ResumeAdmittedWithProgress(ctx, id)
	if update := scheduleReceive(t, ctx, updates); update.RunID != id || update.Phase != StartQueued {
		t.Fatalf("recovery was not queued: %+v", update)
	}
	awaitOccupancy(t, ctx, pool, 1, 1)
	if _, err := a.CommitPendingResult(ctx, id); !errors.Is(err, ErrSessionResultUnavailable) {
		t.Fatalf("save-only resumed an unfinished Run: %v", err)
	}
	path := pendingTestCheckpoint(t, root, id)
	if err := os.Rename(path, path+".offline"); err != nil {
		t.Fatal(err)
	}
	close(gate)
	waitSessionRun(t, ctx, active, contract.Succeeded)
	if update := scheduleReceive(t, ctx, updates); update.RunID != id || update.Phase != StartAdmitting {
		t.Fatalf("missing grant: %+v", update)
	}
	if update := scheduleReceive(t, ctx, updates); update.Phase != StartResolved || update.Run != nil || !errors.Is(update.Err, contract.ErrRunNotFound) {
		t.Fatalf("grant continued without its original checkpoint: %+v", update)
	}
	awaitOccupancy(t, ctx, pool, 0, 0)
	if p, err := a.InspectPending(ctx); err != nil || p.State != RecoveryUnknown || p.RunID != id {
		t.Fatalf("original reservation lost: %+v, %v", p, err)
	}
	if calls := readRecoveryLog[contract.ModelRequest](t, root, "schedule-dispatched"); len(calls) != 1 || calls[0].RunID != active.ID() {
		t.Fatalf("lost checkpoint caused a dispatch: %+v", calls)
	}
}

func pendingTestCheckpoint(t *testing.T, root string, id contract.RunID) string {
	t.Helper()
	key, err := durable.Digest(struct {
		Runner  contract.RunnerRef
		Subject contract.SubjectID
		ID      contract.RunID
	}{"main", "owner", id})
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "runs", key+".run")
}
