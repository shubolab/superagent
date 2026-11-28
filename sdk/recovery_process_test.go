package sdk

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/mesh"
	run "github.com/shubolab/superagent/runner"
)

const recoveryID = "original-run"

type recoveryFixture struct {
	root, barrier, version string
	workspaceWait          time.Duration
	resultRetention        time.Duration
	subject                contract.SubjectID
	prefix                 string
	modelRequests          uint64
	blockWorkspace         bool
	mismatchedReceipt      bool
	unexecutedReceipt      bool
	twoTools               bool
	artifact               bool
	takeover               *takeoverFixture
	mu                     sync.Mutex
}

func (f *recoveryFixture) stopAt(point string) {
	if f.barrier == point {
		fmt.Println("READY " + point)
		// A child parked here must be killed, without closing the SDK or provider.
		for {
			time.Sleep(time.Hour)
		}
	}
}

func (f *recoveryFixture) append(name string, value any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	if err := appendRecoveryFile(filepath.Join(f.root, name), string(data)+"\n"); err != nil {
		panic(err)
	}
}

func appendRecoveryFile(path, line string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString(line)
	return errors.Join(writeErr, file.Sync(), file.Close())
}

func (f *recoveryFixture) Generate(_ context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
	f.append("models", req)
	f.stopAt("model-in-flight")
	reply := &contract.ModelReply{Finish: contract.ModelFinishStop, Text: "done", Usage: &contract.Usage{InputTokens: 2, OutputTokens: 3}}
	if req.Step == 1 {
		reply = &contract.ModelReply{
			Finish: contract.ModelFinishToolCalls, Text: "save", Usage: &contract.Usage{InputTokens: 5, OutputTokens: 7},
			ToolCalls: []contract.ModelToolCall{{CallID: "c1", Tool: "write", Arguments: json.RawMessage(`{"path": "notes.txt"}`)}},
		}
		if f.twoTools {
			reply.ToolCalls = append(reply.ToolCalls, contract.ModelToolCall{CallID: "c2", Tool: "write", Arguments: json.RawMessage(`{"path":"notes.txt"}`)})
		}
	}
	return &completionStream{reply}, nil
}

func (*recoveryFixture) ContextLimits() contract.ModelContextLimits {
	return contract.ModelContextLimits{WindowTokens: 1024}
}

func (f *recoveryFixture) MeasureInput(_ context.Context, req contract.ModelRequest) (contract.InputTokenMeasurement, error) {
	if req.Step == 1 {
		f.stopAt("initial")
	} else {
		f.stopAt("batch-committed")
		if f.takeover != nil && f.takeover.role == "old" && f.takeover.scenario == "batch-committed" {
			if err := writeTakeoverMarker(f.root, "old-at-barrier", "batch durable"); err != nil {
				return contract.InputTokenMeasurement{}, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			if err := waitTakeoverMarker(ctx, f.root, "release-old"); err != nil {
				return contract.InputTokenMeasurement{}, err
			}
		}
	}
	return contract.InputTokenMeasurement{Tokens: 32, Kind: contract.MeasurementExact, ProfileVersion: "fixture", InputDigest: "input"}, nil
}

func (f *recoveryFixture) invoke(ctx context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
	key, ok := InvocationReceiptKey(ctx)
	if !ok {
		return contract.ToolResult{}, errors.New("missing receipt identity")
	}
	f.append("invoked", key)
	if f.takeover != nil {
		return f.takeover.invoke(ctx, req, key)
	}
	if f.unexecutedReceipt {
		return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionNotExecuted, Reason: &contract.ToolReason{Code: "already_fenced"}}, nil
	}
	if err := appendRecoveryFile(filepath.Join(f.root, "workspace", "notes.txt"), "saved:"+string(req.CallID)+"\n"); err != nil {
		return contract.ToolResult{}, err
	}
	f.stopAt("write-without-receipt")
	complete := &contract.ToolCompletion{Outcome: contract.ToolSucceeded, Content: "saved"}
	if f.artifact {
		writer, err := OpenToolArtifact(ctx, req)
		if err != nil {
			return contract.ToolResult{}, err
		}
		defer writer.Close()
		if _, err := writer.Write([]byte(retainedPayload)); err != nil {
			return contract.ToolResult{}, err
		}
		ref, err := writer.Complete()
		if err != nil {
			return contract.ToolResult{}, err
		}
		complete.Artifacts = []contract.ArtifactRef{ref}
	}
	return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionKnown, Completion: complete}, nil
}

type recoveryTransport struct {
	mesh.MeshTransport
	fixture *recoveryFixture
}

func (m recoveryTransport) Call(ctx context.Context, req mesh.MeshRequest) (mesh.MeshReply, error) {
	f := m.fixture
	if f.takeover != nil {
		f.takeover.captureCall(m.MeshTransport, ctx, req)
		if err := f.takeover.beforeCall(ctx, req); err != nil {
			return mesh.MeshReply{}, err
		}
	}
	switch req.Operation {
	case mesh.Invoke:
		f.stopAt("before-delivery")
		reply, err := m.MeshTransport.Call(ctx, req)
		if err == nil && reply.Tool.State != "" {
			f.stopAt("after-receipt")
		}
		return reply, err
	case mesh.QueryReceipt:
		f.append("queries", req.ReceiptKey)
		f.stopAt("reconciling")
		reply, err := m.MeshTransport.Call(ctx, req)
		if f.mismatchedReceipt && reply.Receipt != nil {
			if f.takeover != nil {
				reply.Receipt.Key.AdmissionEpoch++
			} else {
				reply.Receipt.Key.Fingerprint = "different-request"
			}
		}
		return reply, err
	case mesh.GrantEnvironment:
		if req.Environment.Call.CallID == "c1" {
			f.stopAt("before-tool-intent")
		} else {
			f.stopAt("result-settled")
		}
	case mesh.VerifyRecoveryWorkspace:
		if f.blockWorkspace {
			return mesh.MeshReply{}, contract.ErrAccessDenied
		}
	}
	return m.MeshTransport.Call(ctx, req)
}

func (m recoveryTransport) OpenStream(ctx context.Context, req mesh.MeshRequest) (mesh.MeshStream, error) {
	if m.fixture.takeover != nil {
		m.fixture.takeover.captureModel(m.MeshTransport, ctx, req)
	}
	return m.MeshTransport.OpenStream(ctx, req)
}

func fixtureClient(t *testing.T, fixture *recoveryFixture) (*Client, contract.StartRequest) {
	t.Helper()
	defaults := contract.Budget{
		Timeout: address(15 * time.Second), MaxModelRequests: address(uint64(2)), MaxModelInputBytes: address(uint64(8192)),
		MaxOutputBytes: address(uint64(16384)), MaxToolRequests: address(uint64(1)), MaxToolScanBytes: address(uint64(16)),
	}
	if fixture.artifact {
		defaults.MaxToolRawOutputBytes = address(uint64(8192))
	}
	if fixture.modelRequests != 0 {
		defaults.MaxModelRequests = address(fixture.modelRequests)
	}
	if fixture.twoTools {
		defaults.MaxToolRequests = address(uint64(2))
	}
	environmentRef := contract.EnvironmentRequirement{Ref: "files", Workspace: "project"}
	retention := fixture.resultRetention
	if retention == 0 {
		retention = time.Minute
	}
	cfg := contract.RunnerConfig{
		AllowedModels: map[contract.SubjectID][]contract.ModelRef{"owner": {"model"}, "other": {"model"}},
		AllowedTools:  map[contract.SubjectID]map[contract.ToolRef]contract.ToolPermission{"owner": {"write": {Environment: environmentRef, ReadPrefix: fixture.prefix, AllowArtifacts: fixture.artifact}}},
		DefaultBudget: defaults, BudgetCaps: defaults, ObserveBufferBytes: 4096, ResultRetention: retention,
		ContextPolicies: map[contract.ModelRef]contract.ModelContextPolicy{"model": {MaxOutputTokens: 20}},
		Recovery:        &contract.RecoveryConfig{Directory: filepath.Join(fixture.root, "runs"), BindingVersion: fixture.version, ReconcileTimeout: fixture.workspaceWait},
	}
	workspace := map[contract.WorkspaceRef]WorkspaceConfig{"project": {
		Root: filepath.Join(fixture.root, "workspace"), ReadPrefixes: []string{".", "other"},
	}}
	var provider contract.EnvironmentProvider
	var err error
	if fixture.takeover != nil {
		cfg.Recovery.Takeover = &contract.TakeoverConfig{LeaseDuration: takeoverLeaseDuration}
		cfg.AllowedTools["owner"]["write"] = contract.ToolPermission{Environment: environmentRef, ReadPrefix: fixture.prefix, AllowArtifacts: fixture.artifact, AllowWorkspaceWrite: true}
		provider, err = NewLocalTakeoverEnvironment(workspace)
	} else {
		provider, err = NewLocalReadEnvironment(workspace)
	}
	if err != nil {
		t.Fatal(err)
	}
	tool, err := NewLocalReceiptTool(filepath.Join(fixture.root, "receipts"), deliveryTool(fixture.invoke))
	if err != nil {
		t.Fatal(err)
	}
	tools := map[contract.ToolRef]contract.LocalToolBinding{"write": {
		Tool: tool, Spec: contract.ToolSpec{InputSchema: json.RawMessage(`{"type":"object"}`), NeedsReadAccess: true, NeedsWorkspaceWrite: fixture.takeover != nil, NeedsArtifactOutput: fixture.artifact,
			Limits: contract.ToolLimits{MaxResultBytes: 1024, MaxScanBytes: 8, MaxRawOutputBytes: 8192}},
		Prepare: func(json.RawMessage) (string, error) { return "notes.txt", nil },
	}}
	environments := map[contract.EnvironmentRef]contract.EnvironmentProvider{"files": provider}
	var local *mesh.Local
	runner, err := run.New(cfg, map[contract.ModelRef]struct{}{"model": {}}, func(ctx context.Context, subject contract.SubjectID, req contract.ModelRequest) (contract.ModelStream, error) {
		return mesh.OpenModel(ctx, recoveryTransport{local, fixture}, subject, req)
	})
	if err != nil {
		t.Fatal(err)
	}
	if fixture.takeover != nil {
		runner.SetTakeoverClock(fixture.takeover.readClock)
	}
	local, err = mesh.NewLocal(map[contract.RunnerRef]mesh.RunnerEndpoint{"runner": runner}, map[contract.ModelRef]contract.ModelProvider{"model": fixture},
		func(context.Context) (contract.SubjectID, error) { return fixture.subject, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := local.SetComponents(tools, environments); err != nil {
		t.Fatal(err)
	}
	if fixture.artifact {
		store, err := NewLocalRetainedArtifactStore(LocalArtifactConfig{
			Directory: filepath.Join(fixture.root, "artifacts"), MaxArtifactBytes: 8192, MaxStoredBytes: 8192,
			MaxArtifacts: 1, MaxPageBytes: 8192, Retention: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		if err := local.SetArtifactStore(store); err != nil {
			t.Fatal(err)
		}
		runner.SetArtifactStoreID(store.(contract.DurableArtifactStore).ArtifactStoreID())
	}
	if err := runner.ConfigureTools("runner", recoveryTransport{local, fixture}, cfg, tools, environments, fixture.artifact); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(&Mesh{local: local}, "runner")
	if err != nil {
		t.Fatal(err)
	}
	return client, contract.StartRequest{ID: recoveryID, Model: "model", Tools: []contract.ToolRef{"write"}, Environment: &environmentRef,
		Recoverable: true, Takeover: fixture.takeover != nil, Input: contract.UserMessage{Text: "save once"}}
}

func makeRecoveryRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "workspace", "other"), 0700); err != nil {
		t.Fatal(err)
	}
	return root
}

func defaultRecoveryFixture(root string) *recoveryFixture {
	return &recoveryFixture{root: root, version: "v1", subject: "owner", prefix: ".", workspaceWait: 300 * time.Millisecond}
}

func TestRecoveryChild(t *testing.T) {
	root := os.Getenv("SUPERAGENT_RECOVERY_TEST_DIR")
	if root == "" {
		return
	}
	f := defaultRecoveryFixture(root)
	f.barrier = os.Getenv("SUPERAGENT_RECOVERY_TEST_BARRIER")
	f.twoTools = os.Getenv("SUPERAGENT_RECOVERY_TEST_TWO_TOOLS") == "1"
	f.artifact = os.Getenv("SUPERAGENT_RECOVERY_TEST_ARTIFACT") == "1"
	f.unexecutedReceipt = os.Getenv("SUPERAGENT_RECOVERY_TEST_UNEXECUTED") == "1"
	if os.Getenv("SUPERAGENT_RECOVERY_TEST_SINGLE_MODEL") == "1" {
		f.modelRequests = 1
	}
	if os.Getenv("SUPERAGENT_RECOVERY_TEST_LONG_WINDOW") == "1" {
		f.workspaceWait = 1800 * time.Millisecond
	}
	client, req := fixtureClient(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	var handle *Run
	var err error
	if os.Getenv("SUPERAGENT_RECOVERY_TEST_MODE") == "start" {
		handle, err = client.Start(ctx, req)
	} else {
		handle, err = client.Recover(ctx, req.ID)
	}
	if err != nil {
		t.Fatal(err)
	}
	first, err := handle.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.stopAt("terminal")
	again, err := client.Recover(ctx, req.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := again.Wait(ctx)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("terminal changed: %+v, %+v (%v)", first, second, err)
	}
	f.append("results", second)
}

func recoveryProcess(t *testing.T, root, mode, barrier string, extra ...string) func() {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryChild$")
	cmd.Env = append(os.Environ(), "SUPERAGENT_RECOVERY_TEST_DIR="+root,
		"SUPERAGENT_RECOVERY_TEST_MODE="+mode, "SUPERAGENT_RECOVERY_TEST_BARRIER="+barrier)
	cmd.Env = append(cmd.Env, extra...)
	if barrier == "" {
		defer cancel()
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("%s process: %v\n%s", mode, err, out)
		}
		return func() {}
	}
	cmd.Stderr = os.Stderr
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	var killOnce sync.Once
	kill := func() {
		killOnce.Do(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			cancel()
		})
	}
	t.Cleanup(kill)
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		if scanner.Text() == "READY "+barrier {
			return kill
		}
		t.Log(scanner.Text())
	}
	kill()
	t.Fatalf("%s process missed barrier %q", mode, barrier)
	return nil
}

func readRecoveryLog[T any](t *testing.T, root, name string) []T {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var values []T
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var value T
		if err := json.Unmarshal([]byte(line), &value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	return values
}

type fixtureCheckpoint struct {
	Deadline, ReconcileUntil time.Time
	Owner                    *struct {
		Epoch, Revision uint64
		Instance        string
	}
	Step          uint32
	ModelInFlight bool
	Pending       *contract.AssistantMessage
	ToolResults   []contract.ToolResult
	Messages      []contract.Message
	WriteEvidence map[contract.CallID]contract.WriteEvidenceRef
	RemoteCleanup contract.CleanupStatus
	Result        *contract.Result
	FinishedAt    time.Time
	Balance       struct{ ModelRequests, ToolRequests, Output, Scan, Raw uint64 }
	Intent        *struct {
		Key    contract.ReceiptKey
		Limits contract.ToolLimits
	}
}

func readRecoveryCheckpoint(t *testing.T, root string) fixtureCheckpoint {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "runs", "*.run"))
	if err != nil || len(files) != 1 {
		t.Fatalf("checkpoint files = %v (%v)", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint fixtureCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		t.Fatal(err)
	}
	return checkpoint
}

func TestLocalRestartRecoveryProcesses(t *testing.T) {
	requireLocalRecovery(t)
	for _, tc := range []struct {
		barrier       string
		outcome       contract.RunOutcome
		stop          contract.StopCode
		calls, models int
		queries       bool
		messages      int
	}{
		{"after-receipt", contract.Succeeded, "", 1, 2, true, 4},
		{"before-delivery", contract.ToolOutcomeUnknown, contract.StopToolUnknown, 0, 1, true, 1},
		{"write-without-receipt", contract.ToolOutcomeUnknown, contract.StopToolUnknown, 1, 1, true, 1},
		{"batch-committed", contract.Succeeded, "", 1, 2, false, 4},
		{"initial", contract.Succeeded, "", 1, 2, false, 4},
		{"model-in-flight", contract.Failed, contract.StopModelInterrupted, 0, 1, false, 1},
		{"before-tool-intent", contract.Succeeded, "", 1, 2, false, 4},
		{"result-settled", contract.Succeeded, "", 2, 2, false, 5},
		{"terminal", contract.Succeeded, "", 1, 2, false, 4},
	} {
		t.Run(tc.barrier, func(t *testing.T) {
			var flags []string
			if tc.barrier == "result-settled" {
				flags = []string{"SUPERAGENT_RECOVERY_TEST_TWO_TOOLS=1"}
			}
			root := makeRecoveryRoot(t)
			kill := recoveryProcess(t, root, "start", tc.barrier, flags...)
			before := readRecoveryCheckpoint(t, root)
			kill()
			recoveryProcess(t, root, "recover", "", flags...)
			results := readRecoveryLog[contract.Result](t, root, "results")
			if len(results) != 1 || results[0].Outcome != tc.outcome || len(results[0].Messages) != tc.messages {
				t.Fatalf("outcome after recovery = %+v", results)
			}
			result := results[0]
			if tc.stop != "" && (result.Stop == nil || result.Stop.Code != tc.stop) {
				t.Fatalf("stop = %+v", result.Stop)
			}
			if tc.outcome == contract.ToolOutcomeUnknown && (result.Stop.CallID != "c1" || result.Stop.Trigger != contract.StopReconcileDeadline) {
				t.Fatalf("unresolved identity = %+v", result.Stop)
			}
			invoked := readRecoveryLog[contract.ReceiptKey](t, root, "invoked")
			models := readRecoveryLog[contract.ModelRequest](t, root, "models")
			queries := readRecoveryLog[contract.ReceiptKey](t, root, "queries")
			content, err := os.ReadFile(filepath.Join(root, "workspace", "notes.txt"))
			wantFile := ""
			if tc.calls > 0 {
				wantFile = "saved:c1\n"
			}
			if tc.calls > 1 {
				wantFile += "saved:c2\n"
			}
			if (err != nil && !errors.Is(err, os.ErrNotExist)) || string(content) != wantFile ||
				len(invoked) != tc.calls || len(models) != tc.models || (len(queries) > 0) != tc.queries {
				t.Fatalf("file %q (%v), calls %d, models %d, queries %d", content, err, len(invoked), len(models), len(queries))
			}
			for _, query := range queries {
				if before.Intent == nil || query != before.Intent.Key || query.Fingerprint == "" {
					t.Fatalf("query did not use original receipt identity: %+v", query)
				}
			}
			after := readRecoveryCheckpoint(t, root)
			if !before.Deadline.Equal(after.Deadline) || after.Balance.ToolRequests != 0 && tc.calls > 0 {
				t.Fatalf("checkpoint changed deadline or refilled budget: before %+v, after %+v", before, after)
			}
			if tc.queries && tc.outcome == contract.ToolOutcomeUnknown &&
				(after.Balance.Output != before.Balance.Output-before.Intent.Limits.MaxResultBytes ||
					after.Balance.Scan != before.Balance.Scan-before.Intent.Limits.MaxScanBytes || after.Balance.ToolRequests != before.Balance.ToolRequests) {
				t.Fatalf("unknown call did not settle its original reservation: before %+v, after %+v", before, after)
			}
			if tc.outcome == contract.Succeeded {
				if models[1].Step != 2 || len(models[1].Messages) != tc.messages-1 || models[1].Messages[2].Tool.Result.CallID != "c1" ||
					result.Usage == nil || result.Usage.InputTokens != 7 || result.Usage.OutputTokens != 10 {
					t.Fatalf("second model did not see original messages and usage: %+v, %+v", models[1], result.Usage)
				}
			}
			if tc.barrier == "after-receipt" {
				payload, _ := json.Marshal(models[1].Messages[2].Tool.Result)
				if len(queries) != 1 || queries[0] != invoked[0] || models[1].MaxOutputBytes != before.Balance.Output-uint64(len(payload)) {
					t.Fatalf("original receipt was not settled exactly once: pending %+v, request %+v", before, models[1])
				}
			}
			if tc.barrier == "batch-committed" && models[1].MaxOutputBytes != before.Balance.Output {
				t.Fatal("already committed receipt was charged again")
			}
		})
	}
}
