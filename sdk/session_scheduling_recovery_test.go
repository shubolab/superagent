package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/schedule"
)

type scheduleRecoveryModel struct {
	root     string
	initial  chan struct{}
	holdGate <-chan struct{}
}

func (m *scheduleRecoveryModel) ContextLimits() contract.ModelContextLimits {
	return contract.ModelContextLimits{WindowTokens: 4096}
}

func (m *scheduleRecoveryModel) MeasureInput(ctx context.Context, req contract.ModelRequest) (contract.InputTokenMeasurement, error) {
	if m.initial != nil && req.Messages[0].User.Text == "checkpoint" {
		close(m.initial)
		<-ctx.Done()
		return contract.InputTokenMeasurement{}, ctx.Err()
	}
	return contract.InputTokenMeasurement{Tokens: 20, Kind: contract.MeasurementExact, ProfileVersion: "test", InputDigest: "input"}, nil
}

func (m *scheduleRecoveryModel) Generate(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
	encoded, err := json.Marshal(req)
	if err == nil {
		err = appendRecoveryFile(filepath.Join(m.root, "schedule-dispatched"), string(encoded)+"\n")
	}
	if err != nil {
		return nil, err
	}
	if m.holdGate != nil && req.Messages[0].User.Text == "holding" {
		return textSessionStream(ctx, "holding", " done", m.holdGate), nil
	}
	return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "done"}}, nil
}

func scheduleRecoveryAssembly(t *testing.T, root string, model *scheduleRecoveryModel) (*Mesh, *schedule.Scheduler) {
	t.Helper()
	limits := &contract.SessionSchedulingConfig{MaxActiveRuns: 1, MaxQueuedRuns: 2, MaxQueueWait: 4 * time.Second}
	return scheduleAssembly(t, root, limits, model, nil, func(cfg *contract.RunnerConfig) {
		cfg.ContextPolicies = map[contract.ModelRef]contract.ModelContextPolicy{"chat": {MaxOutputTokens: 20}}
	})
}

func runScheduleSource(t *testing.T, ctx context.Context, root string) {
	initial := make(chan struct{})
	assembly, pool := scheduleRecoveryAssembly(t, root, &scheduleRecoveryModel{root: root, initial: initial})
	c := scheduledClient(t, assembly)
	a, b := openTestSession(t, ctx, c, root, "A"), openTestSession(t, ctx, c, root, "B")
	first, queued := sessionRequest(a, "checkpoint"), sessionRequest(b, "queued")
	startSessionRun(t, ctx, a, first)
	scheduleReceive(t, ctx, initial)
	beginScheduled(ctx, b, queued)
	awaitOccupancy(t, ctx, pool, 1, 1)
	assertUnadmitted(t, ctx, c, b, queued.ID)
	sessionLog(t, root, "schedule-ids", []contract.RunID{first.ID, queued.ID})
	(&recoveryFixture{barrier: "schedule-queued"}).stopAt("schedule-queued")
}

func TestSessionSchedulingProcessRestart(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	runSessionChild(t, root, "schedule-source", "schedule-queued")
	ids := readRecoveryLog[[]contract.RunID](t, root, "schedule-ids")[0]
	if old := readRecoveryLog[contract.ModelRequest](t, root, "schedule-dispatched"); len(old) != 0 {
		t.Fatalf("original process dispatched before measured boundary: %+v", old)
	}
	assembly, pool := scheduleRecoveryAssembly(t, root, &scheduleRecoveryModel{root: root})
	c := scheduledClient(t, assembly)
	a, b := openTestSession(t, ctx, c, root, "A"), openTestSession(t, ctx, c, root, "B")
	awaitOccupancy(t, ctx, pool, 0, 0)
	assertUnadmitted(t, ctx, c, b, ids[1])
	if _, err := b.Recover(ctx, ids[1]); !errors.Is(err, ErrSessionOwnership) {
		t.Fatalf("pure waiting request was durable: %v", err)
	}
	run, err := a.Recover(ctx, ids[0])
	if err != nil || run.ID() != ids[0] {
		t.Fatalf("original A recovery: %v, %v", run, err)
	}
	waitSessionRun(t, ctx, run, contract.Succeeded)
	commitSessionRun(t, ctx, a, run.ID(), 1)
	if calls := readRecoveryLog[contract.ModelRequest](t, root, "schedule-dispatched"); len(calls) != 1 || calls[0].RunID != ids[0] || calls[0].Step != 1 {
		t.Fatalf("original recovery or ghost B execution: %+v", calls)
	}
	request := sessionRequest(b, "queued")
	request.ID = ids[1]
	newRun := startSessionRun(t, ctx, b, request)
	waitSessionRun(t, ctx, newRun, contract.Succeeded)
	commitSessionRun(t, ctx, b, ids[1], 1)
	if calls := readRecoveryLog[contract.ModelRequest](t, root, "schedule-dispatched"); len(calls) != 2 || calls[1].RunID != ids[1] {
		t.Fatalf("explicit resubmit: %+v", calls)
	}
}

func TestSessionSchedulingRecoveryGate(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	runSessionChild(t, root, "schedule-source", "schedule-queued")
	original := readRecoveryLog[[]contract.RunID](t, root, "schedule-ids")[0][0]
	holdGate := make(chan struct{})
	release := sync.OnceFunc(func() { close(holdGate) })
	defer release()
	assembly, pool := scheduleRecoveryAssembly(t, root, &scheduleRecoveryModel{root: root, holdGate: holdGate})
	c := scheduledClient(t, assembly)
	a := openTestSession(t, ctx, c, root, "A")
	terminal := openTestSession(t, ctx, c, root, "terminal")
	sealed := openTestSession(t, ctx, c, root, "sealed")
	holding := openTestSession(t, ctx, c, root, "holding")
	terminalRun := startSessionRun(t, ctx, terminal, sessionRequest(terminal, "terminal"))
	waitSessionRun(t, ctx, terminalRun, contract.Succeeded)
	sealedRun := startSessionRun(t, ctx, sealed, sessionRequest(sealed, "sealed"))
	sealedResult := waitSessionRun(t, ctx, sealedRun, contract.Succeeded)
	func() {
		lock, err := fileSessionStore(sealed).Lock(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Close()
		idx, err := fileSessionStore(sealed).Load()
		if err != nil {
			t.Fatal(err)
		}
		if _, committed, err := fileSessionStore(sealed).PrepareCommit(idx, sealedRun.ID(), &sealedResult); err != nil || committed {
			t.Fatalf("seal terminal: %v, %v", committed, err)
		}
	}()

	holdingRun := startSessionRun(t, ctx, holding, sessionRequest(holding, "holding"))
	events := observeSessionRun(t, ctx, holdingRun)
	recvSessionEvent(t, events, holdingRun.ID(), contract.EventTextDelta)
	awaitOccupancy(t, ctx, pool, 1, 0)
	if _, err := c.Recover(ctx, original); !errors.Is(err, contract.ErrScheduleAdmissionRequired) {
		t.Fatalf("direct recovery bypassed the gate: %v", err)
	}
	if _, err := c.Start(ctx, contract.StartRequest{ID: "raw", Model: "chat", Recoverable: true}); !errors.Is(err, contract.ErrScheduleAdmissionRequired) {
		t.Fatalf("direct start bypassed the gate: %v", err)
	}
	if originalHandle, err := holding.Recover(ctx, holdingRun.ID()); err != nil || originalHandle.ID() != holdingRun.ID() {
		t.Fatalf("active recovery did not reuse original Run: %v, %v", originalHandle, err)
	}
	awaitOccupancy(t, ctx, pool, 1, 0)
	commitSessionRun(t, ctx, terminal, terminalRun.ID(), 1)
	commitSessionRun(t, ctx, sealed, sealedRun.ID(), 1)
	type commitResult struct {
		receipt SessionCommit
		err     error
	}
	done := make(chan commitResult, 1)
	go func() {
		receipt, err := a.CommitRun(ctx, original)
		done <- commitResult{receipt, err}
	}()
	awaitOccupancy(t, ctx, pool, 1, 1)
	assertSessionVersion(t, ctx, a, 0, 0)
	if calls := readRecoveryLog[contract.ModelRequest](t, root, "schedule-dispatched"); len(calls) != 3 {
		t.Fatalf("gated recovery dispatched early: %+v", calls)
	}
	release()
	waitSessionRun(t, ctx, holdingRun, contract.Succeeded)
	result := scheduleReceive(t, ctx, done)
	if result.err != nil || result.receipt.RunID != original || result.receipt.Version != 1 {
		t.Fatalf("recovered commit: %+v", result)
	}
	if calls := readRecoveryLog[contract.ModelRequest](t, root, "schedule-dispatched"); len(calls) != 4 || calls[3].RunID != original {
		t.Fatalf("expected one original recovery dispatch: %+v", calls)
	}
	awaitOccupancy(t, ctx, pool, 0, 0)
}
