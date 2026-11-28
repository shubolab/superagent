package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/mesh"
)

const takeoverLeaseDuration = 30 * time.Second

type capturedCall struct {
	transport mesh.MeshTransport
	ctx       context.Context
	request   mesh.MeshRequest
}

type takeoverFixture struct {
	root, role, scenario string
	mu                   sync.Mutex
	model, grant, tool   capturedCall
}

func (f *takeoverFixture) readClock() (time.Time, error) {
	if f.role == "old" {
		if _, err := os.Stat(filepath.Join(f.root, "partition-old")); err == nil {
			return time.Time{}, errors.New("simulated authority disconnection")
		}
	}
	data, err := os.ReadFile(filepath.Join(f.root, "clock"))
	if err != nil {
		return time.Time{}, err
	}
	ns, err := strconv.ParseInt(string(data), 10, 64)
	return time.Unix(0, ns), err
}

func (f *takeoverFixture) captureCall(tr mesh.MeshTransport, ctx context.Context, req mesh.MeshRequest) {
	if f.role != "old" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch req.Operation {
	case mesh.Invoke:
		f.tool = capturedCall{tr, ctx, req}
	case mesh.GrantEnvironment:
		f.grant = capturedCall{tr, ctx, req}
	}
}

func (f *takeoverFixture) captureModel(tr mesh.MeshTransport, ctx context.Context, req mesh.MeshRequest) {
	if f.role != "old" || req.Operation != mesh.Generate {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.model = capturedCall{tr, ctx, req}
}

func (f *takeoverFixture) beforeCall(ctx context.Context, req mesh.MeshRequest) error {
	if f.role == "middle" && req.Operation == mesh.QueryReceipt {
		if err := writeTakeoverMarker(f.root, "middle-reconcile", "query"); err != nil {
			return err
		}
		return waitTakeoverMarker(ctx, f.root, "release-middle")
	}
	return nil
}

func (f *takeoverFixture) invoke(ctx context.Context, req contract.ToolRequest, key contract.ReceiptKey) (contract.ToolResult, error) {
	empty := contract.ToolResult{}
	if f.role != "old" || key.AdmissionEpoch != 1 {
		return empty, fmt.Errorf("replayed invocation in %s epoch %d", f.role, key.AdmissionEpoch)
	}
	wroteFirst := f.scenario == "batch-committed" || f.scenario == "late-receipt" || f.scenario == "receipt-after-terminal" || f.scenario == "wrong-epoch" || f.scenario == "unfenced-workspace"
	if wroteFirst {
		if err := PublishToolFile(ctx, req, "notes.txt", []byte("saved:c1\n")); err != nil {
			return empty, err
		}
	}
	if f.scenario == "batch-committed" {
		return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionKnown,
			Completion: &contract.ToolCompletion{Outcome: contract.ToolSucceeded, Content: "saved"}}, nil
	}
	var artifact contract.ArtifactWriter
	if f.scenario == "fenced-artifact" {
		var err error
		artifact, err = OpenToolArtifact(ctx, req)
		if err != nil {
			return empty, err
		}
		defer artifact.Close()
		if _, err := artifact.Write([]byte("staged")); err != nil {
			return empty, err
		}
	}
	if err := writeTakeoverMarker(f.root, "old-at-barrier", "alive"); err != nil {
		return empty, err
	}
	wait, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()
	if err := waitTakeoverMarker(wait, f.root, "release-old"); err != nil {
		return empty, err
	}
	if err := f.staleRequests(); err != nil {
		return empty, err
	}
	if err := PublishToolFile(ctx, req, "notes.txt", []byte("stale:c1\n")); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
		return empty, fmt.Errorf("stale workspace publish got %v", err)
	}
	if artifact != nil {
		if _, err := artifact.Complete(); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
			return empty, fmt.Errorf("stale artifact publication got %v", err)
		}
	}
	if err := writeTakeoverMarker(f.root, "stale-fenced", "model, tool, grant, workspace"); err != nil {
		return empty, err
	}
	switch {
	case wroteFirst:
		return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionKnown,
			Completion: &contract.ToolCompletion{Outcome: contract.ToolSucceeded, Content: "saved"}}, nil
	case f.scenario == "fenced-not-executed":
		return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionNotExecuted,
			Reason: &contract.ToolReason{Code: "publish_fenced"}}, nil
	default:
		return empty, errors.New("no trustworthy tool receipt")
	}
}

func (f *takeoverFixture) staleRequests() error {
	f.mu.Lock()
	model, tool, grant := f.model, f.tool, f.grant
	f.mu.Unlock()
	if model.transport == nil || tool.transport == nil || grant.transport == nil {
		return errors.New("missing original request in live old process")
	}
	if _, err := model.transport.OpenStream(context.WithoutCancel(model.ctx), model.request); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
		return fmt.Errorf("stale model request got %v", err)
	}
	for _, call := range []capturedCall{tool, grant} {
		if _, err := call.transport.Call(context.WithoutCancel(call.ctx), call.request); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
			return fmt.Errorf("stale request %v got %v", call.request.Operation, err)
		}
	}
	e := grant.request.Environment
	e.Run.Epoch++
	e.Call = contract.CallScope{}
	if _, err := mesh.CallComponent(context.WithoutCancel(grant.ctx), grant.transport, mesh.ReleaseEnvironment, e, contract.ToolRequest{}); !errors.Is(err, contract.ErrAccessDenied) {
		return fmt.Errorf("old guard released a different generation: %v", err)
	}
	return nil
}

func newTakeoverFixture(root, role, scenario string) *recoveryFixture {
	f := defaultRecoveryFixture(root)
	f.workspaceWait = 700 * time.Millisecond
	f.takeover = &takeoverFixture{root: root, role: role, scenario: scenario}
	f.mismatchedReceipt = role != "old" && scenario == "wrong-epoch"
	f.blockWorkspace = role != "old" && scenario == "unfenced-workspace"
	f.artifact = scenario == "fenced-artifact"
	if scenario == "second-takeover" {
		f.workspaceWait = 2500 * time.Millisecond
	}
	return f
}

func TestTakeoverChild(t *testing.T) {
	root := os.Getenv("SUPERAGENT_TAKEOVER_TEST_DIRECTORY")
	if root == "" {
		return
	}
	role := os.Getenv("SUPERAGENT_TAKEOVER_TEST_ROLE")
	scenario := os.Getenv("SUPERAGENT_TAKEOVER_TEST_SCENARIO")
	f := newTakeoverFixture(root, role, scenario)
	client, req := fixtureClient(t, f)
	req.Budget.Timeout = address(10 * time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if role == "old" {
		handle, err := client.Start(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := handle.Wait(ctx); !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
			t.Fatalf("old checkpoint write = %v", err)
		}
		if err := writeTakeoverMarker(root, "old-checkpoint-rejected", "still alive"); err != nil {
			t.Fatal(err)
		}
		if err := waitTakeoverMarker(ctx, root, "finish-old"); err != nil {
			t.Fatal(err)
		}
		return
	}
	handle, err := client.Recover(ctx, req.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTakeoverMarker(root, role+"-owns", string(handle.ID())); err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(ctx)
	if role == "middle" {
		if !errors.Is(err, contract.ErrRecoveryOwnershipLost) {
			t.Fatalf("middle old checkpoint = %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	again, err := client.Recover(ctx, req.ID)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := again.Wait(ctx)
	if err != nil || !reflect.DeepEqual(result, repeated) {
		t.Fatalf("repeated terminal = %+v (%v)", repeated, err)
	}
	f.append("results", result)
	if err := writeTakeoverMarker(root, "new-finished", string(result.Outcome)); err != nil {
		t.Fatal(err)
	}
}

func writeTakeoverMarker(root, name, value string) error {
	file, err := os.CreateTemp(root, ".signal-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, err = file.WriteString(value)
	if err := errors.Join(err, file.Close()); err != nil {
		return err
	}
	return os.Rename(file.Name(), filepath.Join(root, name))
}

func waitTakeoverMarker(ctx context.Context, root, name string) error {
	ticker := time.NewTicker(4 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(root, name)); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func launchTakeoverProcess(t *testing.T, root, role, scenario string) (*os.Process, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTakeoverChild$")
	cmd.Env = append(os.Environ(), "SUPERAGENT_TAKEOVER_TEST_DIRECTORY="+root,
		"SUPERAGENT_TAKEOVER_TEST_ROLE="+role, "SUPERAGENT_TAKEOVER_TEST_SCENARIO="+scenario)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	var exit error
	go func() { exit = cmd.Wait(); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	wait := func() {
		<-done
		if exit != nil {
			t.Fatalf("%s process failed: %v", role, exit)
		}
	}
	return cmd.Process, wait
}

func TestLocalRunnerTakeoverProcesses(t *testing.T) {
	requireLocalRecovery(t)
	for _, tc := range []struct {
		scenario string
		outcome  contract.RunOutcome
		saved    bool
		late     bool
	}{
		{"late-receipt", contract.Succeeded, true, false},
		{"fenced-not-executed", contract.Succeeded, false, false},
		{"fenced-no-receipt", contract.ToolOutcomeUnknown, false, false},
		{"fenced-artifact", contract.ToolOutcomeUnknown, false, false},
		{"receipt-after-terminal", contract.ToolOutcomeUnknown, true, true},
		{"wrong-epoch", contract.ToolOutcomeUnknown, true, false},
		{"unfenced-workspace", contract.Failed, true, false},
	} {
		t.Run(tc.scenario, func(t *testing.T) {
			root := makeRecoveryRoot(t)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			mark := func(name, value string) {
				t.Helper()
				if err := writeTakeoverMarker(root, name, value); err != nil {
					t.Fatal(err)
				}
			}
			wait := func(name string) {
				t.Helper()
				if err := waitTakeoverMarker(ctx, root, name); err != nil {
					t.Fatal(err)
				}
			}
			now := time.Now()
			mark("clock", strconv.FormatInt(now.UnixNano(), 10))
			old, oldDone := launchTakeoverProcess(t, root, "old", tc.scenario)
			wait("old-at-barrier")
			before := readRecoveryCheckpoint(t, root)
			mark("partition-old", "")
			mark("clock", strconv.FormatInt(now.Add(takeoverLeaseDuration+time.Second).UnixNano(), 10))
			_, newDone := launchTakeoverProcess(t, root, "new", tc.scenario)
			wait("new-owns")
			if err := os.Remove(filepath.Join(root, "partition-old")); err != nil {
				t.Fatal(err)
			}
			if tc.late {
				wait("new-finished")
			}
			mark("release-old", "")
			wait("stale-fenced")
			wait("old-checkpoint-rejected")
			if err := old.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("old runner exited instead of being fenced: %v", err)
			}
			wait("new-finished")
			newDone()
			mark("finish-old", "")
			oldDone()
			results := readRecoveryLog[contract.Result](t, root, "results")
			models := readRecoveryLog[contract.ModelRequest](t, root, "models")
			invoked := readRecoveryLog[contract.ReceiptKey](t, root, "invoked")
			queries := readRecoveryLog[contract.ReceiptKey](t, root, "queries")
			bytes, err := os.ReadFile(filepath.Join(root, "workspace", "notes.txt"))
			wantFile := ""
			if tc.saved {
				wantFile = "saved:c1\n"
			}
			if (err != nil && !errors.Is(err, os.ErrNotExist)) || string(bytes) != wantFile ||
				len(results) != 1 || results[0].Outcome != tc.outcome || len(invoked) != 1 ||
				invoked[0].AdmissionEpoch != 1 || len(queries) == 0 {
				t.Fatalf("result %v, writes %q (%v), invocations %v, queries %v", results, bytes, err, invoked, queries)
			}
			for _, query := range queries {
				if query != invoked[0] {
					t.Fatal("new runner used a different original invocation")
				}
			}
			after := readRecoveryCheckpoint(t, root)
			if before.Owner == nil || after.Owner == nil || before.Owner.Epoch != 1 || after.Owner.Epoch != 2 ||
				before.Owner.Instance == after.Owner.Instance || !before.Deadline.Equal(after.Deadline) ||
				after.Balance.ToolRequests != before.Balance.ToolRequests || after.Balance.ToolRequests != 0 {
				t.Fatalf("ownership or original budget changed incorrectly: before %+v after %+v", before, after)
			}
			if tc.outcome == contract.Succeeded {
				if len(models) != 2 || len(models[1].Messages) != 3 || len(results[0].Messages) != 4 ||
					models[1].Messages[2].Tool.Result.CallID != "c1" {
					t.Fatalf("tool results not paired once: models %v result %v", models, results)
				}
				paired := models[1].Messages[2].Tool.Result
				size := jsonSize(t, paired)
				if models[1].MaxOutputBytes != before.Balance.Output-size {
					t.Fatalf("receipt budget was not settled once: model %v", models[1])
				}
				if tc.scenario == "fenced-not-executed" && paired.State != contract.ToolExecutionNotExecuted {
					t.Fatalf("missing definitive not-executed receipt: %+v", paired)
				}
			} else if len(models) != 1 || len(results[0].Messages) != 1 || (tc.outcome == contract.ToolOutcomeUnknown &&
				(results[0].Stop.CallID != "c1" || after.Balance.Output != before.Balance.Output-before.Intent.Limits.MaxResultBytes)) {
				t.Fatalf("uncertain outcome advanced or reset the original budget: %+v, models %v", results[0], models)
			}
			if tc.late {
				client, req := fixtureClient(t, newTakeoverFixture(root, "reader", tc.scenario))
				handle, err := client.Recover(ctx, req.ID)
				if err != nil {
					t.Fatal(err)
				}
				result, err := handle.Wait(ctx)
				if err != nil || !reflect.DeepEqual(result, results[0]) {
					t.Fatalf("late receipt changed terminal: %+v (%v)", result, err)
				}
			}
		})
	}
}

func jsonSize(t *testing.T, value any) uint64 {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(len(b))
}

func TestTakeoverFromCommittedBatch(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	mark := func(name, value string) {
		t.Helper()
		if err := writeTakeoverMarker(root, name, value); err != nil {
			t.Fatal(err)
		}
	}
	wait := func(name string) {
		t.Helper()
		if err := waitTakeoverMarker(ctx, root, name); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	mark("clock", strconv.FormatInt(now.UnixNano(), 10))
	old, oldDone := launchTakeoverProcess(t, root, "old", "batch-committed")
	wait("old-at-barrier")
	before := readRecoveryCheckpoint(t, root)
	if before.Step != 2 || before.Intent != nil {
		t.Fatalf("fixture did not pause at the committed batch: %+v", before)
	}
	mark("clock", strconv.FormatInt(now.Add(takeoverLeaseDuration+time.Second).UnixNano(), 10))
	_, newDone := launchTakeoverProcess(t, root, "new", "batch-committed")
	wait("new-owns")
	mark("release-old", "")
	wait("old-checkpoint-rejected")
	wait("new-finished")
	if err := old.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("old runner exited: %v", err)
	}
	mark("finish-old", "")
	oldDone()
	newDone()
	models := readRecoveryLog[contract.ModelRequest](t, root, "models")
	queries := readRecoveryLog[contract.ReceiptKey](t, root, "queries")
	invoked := readRecoveryLog[contract.ReceiptKey](t, root, "invoked")
	if len(models) != 2 || models[1].Step != 2 || models[1].MaxOutputBytes != before.Balance.Output ||
		len(models[1].Messages) != 3 || models[1].Messages[2].Tool.Result.CallID != "c1" || len(queries) != 0 || len(invoked) != 1 {
		t.Fatalf("committed batch was replayed: model %v, query %v, calls %v", models, queries, invoked)
	}
}

func TestTakeoverPreservesReconcileDeadlineAcrossOwners(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	mark := func(name, value string) {
		t.Helper()
		if err := writeTakeoverMarker(root, name, value); err != nil {
			t.Fatal(err)
		}
	}
	wait := func(name string) {
		t.Helper()
		if err := waitTakeoverMarker(ctx, root, name); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	mark("clock", strconv.FormatInt(now.UnixNano(), 10))
	_, oldDone := launchTakeoverProcess(t, root, "old", "second-takeover")
	wait("old-at-barrier")
	mark("clock", strconv.FormatInt(now.Add(takeoverLeaseDuration+time.Second).UnixNano(), 10))
	_, middleDone := launchTakeoverProcess(t, root, "middle", "second-takeover")
	wait("middle-reconcile")
	before := readRecoveryCheckpoint(t, root)
	if before.Owner.Epoch != 2 || before.ReconcileUntil.IsZero() {
		t.Fatalf("second owner did not persist deadline: %+v", before)
	}
	mark("clock", strconv.FormatInt(now.Add(2*takeoverLeaseDuration+2*time.Second).UnixNano(), 10))
	_, newDone := launchTakeoverProcess(t, root, "new", "second-takeover")
	wait("new-owns")
	mark("release-middle", "")
	mark("release-old", "")
	wait("old-checkpoint-rejected")
	wait("new-finished")
	mark("finish-old", "")
	oldDone()
	middleDone()
	newDone()
	after := readRecoveryCheckpoint(t, root)
	results := readRecoveryLog[contract.Result](t, root, "results")
	if after.Owner.Epoch != 3 || !before.ReconcileUntil.Equal(after.ReconcileUntil) || !before.Deadline.Equal(after.Deadline) ||
		before.Balance.ToolRequests != after.Balance.ToolRequests ||
		len(readRecoveryLog[contract.ReceiptKey](t, root, "invoked")) != 1 || len(results) != 1 || results[0].Outcome != contract.ToolOutcomeUnknown {
		t.Fatalf("reconciliation changed across takeover: before %+v, after %+v, result %v", before, after, results)
	}
}

func TestTakeoverOptInThroughPublicMesh(t *testing.T) {
	requireLocalRecovery(t)
	dir := t.TempDir()
	budget := contract.Budget{
		Timeout: address(time.Minute), MaxModelRequests: address(uint64(1)),
		MaxModelInputBytes: address(uint64(8192)), MaxOutputBytes: address(uint64(4096)),
	}
	calls := 0
	cfg := MeshConfig{
		LocalRunners: map[contract.RunnerRef]contract.RunnerConfig{"runner": {
			AllowedModels: map[contract.SubjectID][]contract.ModelRef{"owner": {"model"}},
			DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 4096, ResultRetention: time.Minute,
			Recovery: &contract.RecoveryConfig{Directory: dir, BindingVersion: "v1", ReconcileTimeout: time.Second, Takeover: &contract.TakeoverConfig{LeaseDuration: time.Minute}},
		}},
		LocalModels: map[contract.ModelRef]contract.ModelProvider{"model": deliveryModel(func(context.Context, contract.ModelRequest) (contract.ModelStream, error) {
			calls++
			return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "done"}}, nil
		})},
		ResolveCaller: func(context.Context) (contract.SubjectID, error) { return "owner", nil },
	}
	newClient := func() *Client {
		m, err := NewMesh(cfg)
		if err != nil {
			t.Fatal(err)
		}
		client, err := NewClient(m, "runner")
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	req := contract.StartRequest{ID: "takeover", Takeover: true, Model: "model"}
	first := newClient()
	if _, err := first.Start(t.Context(), req); !errors.Is(err, contract.ErrInvalidRequest) {
		t.Fatalf("takeover without recoverable opt-in: %v", err)
	}
	req.Recoverable = true
	handle, err := first.Start(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	want, err := handle.Wait(t.Context())
	if err != nil || want.Outcome != contract.Succeeded {
		t.Fatalf("Start = %+v (%v)", want, err)
	}
	recovered, err := newClient().Recover(t.Context(), req.ID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := recovered.Wait(t.Context())
	if err != nil || !reflect.DeepEqual(got, want) || calls != 1 {
		t.Fatalf("Recover = %+v (%v), model calls = %d", got, err, calls)
	}
}
