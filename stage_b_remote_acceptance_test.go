package superagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/run"
)

type stageBRunnerConfig struct {
	Root, Endpoint, Role, Barrier string
	TwoTools, LiveOld, Cancel     bool
	ModelRequests                 uint64
	Window                        time.Duration
}

type stageBModel struct {
	*recoveryFixture
	cfg stageBRunnerConfig
}

func (m stageBModel) Generate(_ context.Context, req ModelRequest) (ModelStream, error) {
	m.append("models", req)
	reply := &ModelReply{Finish: ModelFinishStop, Text: "done"}
	if req.Step == 1 {
		reply = &ModelReply{Finish: ModelFinishToolCalls, Text: "save",
			ToolCalls: []ModelToolCall{{CallID: "c1", Tool: "write", Arguments: json.RawMessage(`{"Path":"notes.txt","Content":"saved:c1\n"}`)}}}
		if m.cfg.TwoTools {
			reply.ToolCalls = append(reply.ToolCalls, ModelToolCall{
				CallID: "c2", Tool: "write", Arguments: json.RawMessage(`{"Path":"audit.txt","Content":"saved:c2\n"}`),
			})
		}
	}
	return &completionStream{reply}, nil
}

func (m stageBModel) MeasureInput(ctx context.Context, req ModelRequest) (InputTokenMeasurement, error) {
	if req.Step == 2 && m.cfg.Barrier == "batch" {
		if err := stageBBarrier(context.WithoutCancel(ctx), m.cfg.Root, "batch"); err != nil {
			return InputTokenMeasurement{}, err
		}
	}
	return m.recoveryFixture.MeasureInput(ctx, req)
}

func stageBBarrier(ctx context.Context, root, name string) error {
	if err := writeTakeoverMarker(root, "stage-b-"+name, "reached"); err != nil {
		return err
	}
	return waitTakeoverMarker(ctx, root, "stage-b-release-"+name)
}

type stageBTransport struct {
	mesh.MeshTransport
	cfg stageBRunnerConfig
	ctx context.Context
	old *takeoverFixture
}

func (m *stageBTransport) Call(ctx context.Context, req mesh.MeshRequest) (mesh.MeshReply, error) {
	if m.old != nil {
		m.old.captureCall(m.MeshTransport, ctx, req)
		if req.Operation == mesh.QueryReceipt {
			if err := stageBBarrier(m.ctx, m.cfg.Root, "old-query"); err != nil {
				return mesh.MeshReply{}, err
			}
		}
	}
	if m.cfg.Barrier == "workspace" && req.Operation == mesh.VerifyRemoteToolWorkspace {
		if err := stageBBarrier(m.ctx, m.cfg.Root, "workspace"); err != nil {
			return mesh.MeshReply{}, err
		}
	}
	reply, err := m.MeshTransport.Call(ctx, req)
	if m.cfg.Cancel && req.Operation == mesh.QueryReceipt {
		if markerErr := writeTakeoverMarker(m.cfg.Root, "stage-b-next-query", "attempted"); markerErr != nil {
			return mesh.MeshReply{}, markerErr
		}
	}
	if err != nil {
		return reply, err
	}
	if m.cfg.Barrier == "receipt" && req.Operation == mesh.QueryReceipt && reply.Receipt != nil && reply.Receipt.Evidence != nil {
		(&recoveryFixture{root: m.cfg.Root}).append("stage-b-delivered-receipts", *reply.Receipt)
		err = stageBBarrier(m.ctx, m.cfg.Root, "receipt")
	}
	if m.cfg.Barrier == "first" && req.Operation == mesh.VerifyRemoteToolWorkspace {
		err = stageBBarrier(m.ctx, m.cfg.Root, "first")
	}
	if m.cfg.Barrier == "terminal" && req.Operation == mesh.Wait {
		err = stageBBarrier(m.ctx, m.cfg.Root, "terminal")
	}
	return reply, err
}

func (m *stageBTransport) OpenStream(ctx context.Context, req mesh.MeshRequest) (mesh.MeshStream, error) {
	if m.old != nil {
		m.old.captureModel(m.MeshTransport, ctx, req)
	}
	return m.MeshTransport.OpenStream(ctx, req)
}

func stageBRunnerClient(t *testing.T, ctx context.Context, cfg stageBRunnerConfig, f *recoveryFixture) (*Client, *stageBTransport) {
	t.Helper()
	_, identity := remoteEvidenceWorkspace(t, cfg.Root)
	model := stageBModel{f, cfg}
	host := remoteRunConfig(t, cfg.Root, cfg.Endpoint, identity, model, cfg.Window, true)
	rc := host.LocalRunners["runner"]
	if cfg.TwoTools {
		rc.DefaultBudget.MaxToolRequests = address(uint64(2))
	}
	if cfg.ModelRequests != 0 {
		rc.DefaultBudget.MaxModelRequests = address(cfg.ModelRequests)
	}
	rc.BudgetCaps = rc.DefaultBudget
	m := &stageBTransport{cfg: cfg, ctx: ctx}
	r, err := run.New(rc, map[ModelRef]struct{}{"model": {}}, func(ctx context.Context, subject SubjectID, req ModelRequest) (ModelStream, error) {
		return mesh.OpenModel(ctx, m, subject, req)
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LiveOld {
		m.old = &takeoverFixture{role: "old"}
		r.SetTakeoverClock(func() (time.Time, error) {
			raw, err := os.ReadFile(filepath.Join(cfg.Root, "stage-b-freeze-clock"))
			if errors.Is(err, os.ErrNotExist) {
				return time.Now(), nil
			}
			if err != nil {
				return time.Time{}, err
			}
			ns, err := strconv.ParseInt(string(raw), 10, 64)
			return time.Unix(0, ns), err
		})
	}
	local, err := mesh.NewLocal(map[RunnerRef]mesh.RunnerEndpoint{"runner": r}, host.LocalModels, host.ResolveCaller)
	if err != nil {
		t.Fatal(err)
	}
	m.MeshTransport = local
	if err := local.SetComponents(host.LocalTools, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.ConfigureTools("runner", m, rc, host.LocalTools, nil, false); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(&Mesh{local: local}, "runner")
	if err != nil {
		t.Fatal(err)
	}
	client.transport = m
	return client, m
}

func TestStageBRemoteRunnerProcess(t *testing.T) {
	raw := os.Getenv("SUPERAGENT_STAGE_B_REMOTE_RUNNER")
	if raw == "" {
		return
	}
	var cfg stageBRunnerConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 14*time.Second)
	defer cancel()
	f := defaultRecoveryFixture(cfg.Root)
	client, transport := stageBRunnerClient(t, ctx, cfg, f)
	var handle *Run
	var err error
	if cfg.Role == "original" {
		handle, err = client.Start(ctx, StartRequest{ID: recoveryID, Model: "model", Recoverable: true, Takeover: true,
			Tools: []ToolRef{"write"}, Input: UserMessage{Text: "save once"}})
	} else {
		handle, err = stageBRecover(ctx, client)
	}
	if err != nil || handle.ID() != recoveryID {
		t.Fatalf("%s admission: %v (%v)", cfg.Role, handle, err)
	}
	if err := writeTakeoverMarker(cfg.Root, "stage-b-accepted-"+cfg.Role, "yes"); err != nil {
		t.Fatal(err)
	}
	if cfg.Cancel {
		if err := waitTakeoverMarker(ctx, cfg.Root, "stage-b-cancel"); err != nil {
			t.Fatal(err)
		}
		if err := handle.Cancel(ctx); err != nil {
			t.Fatal(err)
		}
	}
	result, err := handle.Wait(ctx)
	if cfg.LiveOld {
		if !errors.Is(err, ErrRecoveryOwnershipLost) {
			t.Fatalf("old checkpoint owner returned %v (%+v)", err, result)
		}
		stageBCheckOldOperations(t, ctx, cfg.Root, transport.old, f)
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	f.append("stage-b-results-"+cfg.Role, result)
}

func stageBRecover(ctx context.Context, client *Client) (*Run, error) {
	for {
		handle, err := client.Recover(ctx, recoveryID)
		if !errors.Is(err, ErrRecoveryBusy) {
			return handle, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func stageBCheckOldOperations(t *testing.T, ctx context.Context, root string, old *takeoverFixture, f *recoveryFixture) {
	t.Helper()
	old.mu.Lock()
	tool, model := old.tool, old.model
	old.mu.Unlock()
	if tool.transport == nil || model.transport == nil {
		t.Fatal("old process did not capture its original dispatches")
	}
	if _, err := tool.transport.Call(context.WithoutCancel(tool.ctx), tool.request); !errors.Is(err, ErrRecoveryOwnershipLost) {
		t.Fatalf("stale tool dispatch: %v", err)
	}
	if stream, err := model.transport.OpenStream(context.WithoutCancel(model.ctx), model.request); !errors.Is(err, ErrRecoveryOwnershipLost) {
		if stream != nil {
			_ = stream.Close()
		}
		t.Fatalf("stale model dispatch: %v", err)
	}
	key := tool.request.ReceiptKey
	for _, msg := range readRecoveryLog[remoteWireRequest](t, root, "remote-sends") {
		if msg.Key != key {
			continue
		}
		authority, err := NewSharedRemoteToolAuthority(filepath.Join(root, "runs"))
		if err != nil {
			t.Fatal(err)
		}
		published := false
		err = authority.PublishRemote(ctx, key, msg.Scope, msg.Owner, func() error {
			published = true
			return nil
		})
		if published || !errors.Is(err, ErrRecoveryOwnershipLost) {
			t.Fatalf("old publication guard: published=%t err=%v", published, err)
		}
		f.append("stage-b-old-fenced", os.Getpid())
		return
	}
	t.Fatal("old remote invocation was not recorded")
}

type stageBProcess struct {
	pid  int
	stop func()
	wait func()
}

func launchStageBProcess(t *testing.T, test string, extra string) stageBProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+test+"$")
	cmd.Env = append(os.Environ(), extra)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	var exit error
	go func() { exit = cmd.Wait(); close(done) }()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return stageBProcess{pid: cmd.Process.Pid, stop: stop, wait: func() {
		<-done
		if exit != nil {
			t.Fatalf("%s child %d: %v", test, cmd.Process.Pid, exit)
		}
	}}
}

func launchStageBRunner(t *testing.T, cfg stageBRunnerConfig) stageBProcess {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return launchStageBProcess(t, "TestStageBRemoteRunnerProcess", "SUPERAGENT_STAGE_B_REMOTE_RUNNER="+string(raw))
}

type stageBRemoteFixture struct {
	t                           *testing.T
	root                        string
	cfg                         stageBRunnerConfig
	proxy                       *remoteFaultProxy
	old                         stageBProcess
	workspace                   EnvironmentProvider
	killTool                    func()
	toolPID, replacementToolPID int
	proof                       contract.WriteEvidence
}

func newStageBRemoteFixture(t *testing.T, liveOld, twoTools bool, modelRequests uint64, window time.Duration) *stageBRemoteFixture {
	t.Helper()
	root := makeRecoveryRoot(t)
	makeRemoteCertificates(t, root)
	env, _ := remoteEvidenceWorkspace(t, root)
	tool, endpoint, stop := launchRemoteService(t, root, "original", "evidence-after-commit")
	proxy := newRemoteProxy(t, root, endpoint, "stage-b-acceptance")
	cfg := stageBRunnerConfig{Root: root, Endpoint: proxy.server.URL, Role: "original", LiveOld: liveOld,
		TwoTools: twoTools, ModelRequests: modelRequests, Window: window}
	return &stageBRemoteFixture{t: t, root: root, cfg: cfg, proxy: proxy,
		old: launchStageBRunner(t, cfg), workspace: env, killTool: stop, toolPID: tool.Process.Pid}
}

func (f *stageBRemoteFixture) await(ctx context.Context, marker string) {
	f.t.Helper()
	if err := waitTakeoverMarker(ctx, f.root, marker); err != nil {
		f.t.Fatal(err)
	}
}

func (f *stageBRemoteFixture) mark(name, value string) {
	f.t.Helper()
	if err := writeTakeoverMarker(f.root, name, value); err != nil {
		f.t.Fatal(err)
	}
}

func (f *stageBRemoteFixture) crashTool(ctx context.Context) fixtureCheckpoint {
	t := f.t
	t.Helper()
	f.await(ctx, "evidence-committed")
	raw, err := os.ReadFile(evidenceFixturePath(t, f.root, "receipts/*.receipt"))
	var pending struct {
		Key    ReceiptKey
		Write  *contract.WriteIntent
		Result *ToolResult
	}
	if err != nil || json.Unmarshal(raw, &pending) != nil || pending.Result != nil || pending.Write == nil || pending.Key.CallID != "c1" {
		t.Fatalf("provider did not stop after publication and before receipt: %q (%v)", raw, err)
	}
	f.killTool()
	notes, err := readManagedNotes(t, f.workspace)
	if err != nil || notes != evidenceLine {
		t.Fatalf("durable publication: %q (%v)", notes, err)
	}
	proof, err := f.workspace.(contract.EvidenceWorkspace).ReadWriteEvidence(contract.WithWriteEvidenceQuery(ctx, *pending.Write), *pending.Write)
	if err != nil || proof.Intent.Key != pending.Key || proof.Version.ID != contract.HashBytes([]byte(notes)) {
		t.Fatalf("durable original proof: %+v (%v)", proof, err)
	}
	f.proof = proof
	marker := "remote-before-query"
	if f.cfg.LiveOld {
		marker = "stage-b-old-query"
	}
	f.await(ctx, marker)
	before := readRecoveryCheckpoint(t, f.root)
	if before.Intent == nil || before.Intent.Key != pending.Key || before.Owner == nil || before.Owner.Epoch != 1 || before.ReconcileUntil.IsZero() {
		t.Fatalf("original pending checkpoint: %+v", before)
	}
	return before
}

func (f *stageBRemoteFixture) restartTool() {
	f.t.Helper()
	tool, endpoint, _ := launchRemoteService(f.t, f.root, "replacement", "evidence-ready")
	f.replacementToolPID = tool.Process.Pid
	f.proxy.target.Store(endpoint)
}

func (f *stageBRemoteFixture) recover(ctx context.Context, role, barrier string, cancel bool) stageBProcess {
	f.t.Helper()
	cfg := f.cfg
	cfg.Role, cfg.Barrier, cfg.LiveOld, cfg.Cancel = role, barrier, false, cancel
	child := launchStageBRunner(f.t, cfg)
	f.await(ctx, "stage-b-accepted-"+role)
	return child
}

func (f *stageBRemoteFixture) result(role string) Result {
	f.t.Helper()
	results := readRecoveryLog[Result](f.t, f.root, "stage-b-results-"+role)
	if len(results) != 1 {
		f.t.Fatalf("%s results: %+v", role, results)
	}
	return results[0]
}

func (f *stageBRemoteFixture) checkOriginalOnce(wantCalls int) {
	t := f.t
	t.Helper()
	sends := readRecoveryLog[remoteWireRequest](t, f.root, "remote-sends")
	executed := readRecoveryLog[ReceiptKey](t, f.root, "remote-executed")
	commits := readRecoveryLog[contract.WriteEvidence](t, f.root, "write-evidence-commits")
	if int(f.proxy.sends.Load()) != wantCalls || len(sends) != wantCalls || len(executed) != wantCalls || len(commits) != wantCalls ||
		sends[0].Key != f.proof.Intent.Key || executed[0] != sends[0].Key || commits[0].Ref != f.proof.Ref {
		t.Fatalf("invoke/execution/publication counts or original identity: sends=%+v executed=%+v commits=%+v", sends, executed, commits)
	}
	if wantCalls == 2 && (sends[1].Key.CallID != "c2" || executed[1] != sends[1].Key || commits[1].Intent.Key != sends[1].Key ||
		commits[1].Intent.Path != "audit.txt" || commits[1].Version.ID != contract.HashBytes([]byte("saved:c2\n"))) {
		t.Fatalf("second call was not executed as declared: %+v", commits[1])
	}
}

func stageBToolResults(t *testing.T, messages []Message) []ToolResult {
	t.Helper()
	var results []ToolResult
	for _, message := range messages {
		if message.Tool != nil {
			results = append(results, message.Tool.Result)
		}
	}
	return results
}

func stageBCheckSettled(t *testing.T, before, after fixtureCheckpoint, proof contract.WriteEvidence, tool ToolResult) {
	t.Helper()
	if tool.Completion == nil {
		t.Fatalf("missing completion: %+v", tool)
	}
	var output contract.AppendFileResult
	if err := json.Unmarshal([]byte(tool.Completion.Content), &output); err != nil ||
		tool.CallID != "c1" || tool.State != ToolExecutionKnown || tool.Completion.Outcome != ToolSucceeded ||
		output.Path != proof.Intent.Path || output.Version != proof.Version.ID || output.SHA256 != proof.Version.ID || output.Bytes != proof.Version.Bytes ||
		after.Intent != nil || after.WriteEvidence["c1"] != proof.Ref || after.RemoteCleanup != CleanupPending ||
		after.Balance.Output != before.Balance.Output-jsonSize(t, tool) ||
		after.Balance.Scan != before.Balance.Scan-before.Intent.Limits.MaxScanBytes || after.Balance.ToolRequests != before.Balance.ToolRequests {
		t.Fatalf("original result, reference, cleanup or budget settled incorrectly: tool=%+v after=%+v", tool, after)
	}
}

func TestStageB1LiveOldRunnerAndRebuiltReceipt(t *testing.T) {
	requireLocalRecovery(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	f := newStageBRemoteFixture(t, true, false, 0, 4*time.Second)
	before := f.crashTool(ctx)
	f.mark("stage-b-freeze-clock", strconv.FormatInt(time.Now().UnixNano(), 10))
	f.proxy.holdQueries.Store(true)
	next := f.recover(ctx, "next", "first", false)
	owned := readRecoveryCheckpoint(t, f.root)
	if owned.Owner == nil || owned.Owner.Epoch != 2 || owned.Owner.Instance == before.Owner.Instance ||
		!owned.ReconcileUntil.Equal(before.ReconcileUntil) || !owned.Deadline.Equal(before.Deadline) {
		t.Fatalf("takeover renewed boundaries or used the wrong owner: %+v", owned)
	}
	f.restartTool()
	f.proxy.holdQueries.Store(false)
	f.await(ctx, "stage-b-first")
	settled := readRecoveryCheckpoint(t, f.root)
	if len(settled.ToolResults) != 1 {
		t.Fatalf("partial checkpoint: %+v", settled)
	}
	stageBCheckSettled(t, before, settled, f.proof, settled.ToolResults[0])
	f.mark("stage-b-release-old-query", "yes")
	f.old.wait()
	if pids := readRecoveryLog[int](t, f.root, "stage-b-old-fenced"); !reflect.DeepEqual(pids, []int{f.old.pid}) {
		t.Fatalf("old process did not attempt operations after takeover: %v", pids)
	}
	if afterOld := readRecoveryCheckpoint(t, f.root); !reflect.DeepEqual(afterOld, settled) {
		t.Fatalf("old operations changed the settled checkpoint: %+v", afterOld)
	}
	f.mark("stage-b-release-first", "yes")
	next.wait()
	result := f.result("next")
	models := readRecoveryLog[ModelRequest](t, f.root, "models")
	if result.Outcome != Succeeded || result.Cleanup != CleanupPending || len(models) != 2 ||
		!reflect.DeepEqual(stageBToolResults(t, models[1].Messages), settled.ToolResults) ||
		models[1].MaxOutputBytes != settled.Balance.Output {
		t.Fatalf("takeover did not continue the committed result: %+v, models %+v", result, models)
	}
	f.checkOriginalOnce(1)
	queries := readRecoveryLog[remoteWireRequest](t, f.root, "remote-queries")
	for _, q := range queries {
		if q.Key != before.Intent.Key || q.Owner.Epoch != 2 || q.Key.AdmissionEpoch != 1 || q.Owner.Instance != owned.Owner.Instance {
			t.Fatalf("query confused original admission and current ownership: %+v", q)
		}
	}
	if len(queries) == 0 {
		t.Fatal("new owner did not query original evidence over mTLS")
	}
	t.Logf("runner pid=%d→%d tool pid=%d→%d epoch=%d→%d c1 queries=%d invoke=1 execution=1 publication=1 settlement=1 cleanup=%s outcome=%s",
		f.old.pid, next.pid, f.toolPID, f.replacementToolPID, before.Owner.Epoch, owned.Owner.Epoch, len(queries), result.Cleanup, result.Outcome)
}
