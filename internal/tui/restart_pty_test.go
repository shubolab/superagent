//go:build darwin || linux

package tui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/creack/pty"
	sdk "github.com/shubolab/superagent"
	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/session"
)

type restartEvent struct {
	Kind   string
	ID     sdk.RunID
	Input  string
	Model  *sdk.ModelRequest    `json:",omitempty"`
	Key    *contract.ReceiptKey `json:",omitempty"`
	Commit *sdk.SessionCommit   `json:",omitempty"`
}

type restartFixture struct {
	root, mode string
}

func (f restartFixture) log(event restartEvent) {
	line, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	file, err := os.OpenFile(filepath.Join(f.root, "audit"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	_, err = file.Write(append(line, '\n'))
	if err := errors.Join(err, file.Sync(), file.Close()); err != nil {
		panic(err)
	}
}

func (f restartFixture) park(kind string, id sdk.RunID) {
	f.log(restartEvent{Kind: kind, ID: id})
	// The parent closes this process without releasing the barrier.
	time.Sleep(time.Minute)
}

func (restartFixture) ContextLimits() sdk.ModelContextLimits {
	return sdk.ModelContextLimits{WindowTokens: 4096}
}

func (f restartFixture) MeasureInput(_ context.Context, req sdk.ModelRequest) (sdk.InputTokenMeasurement, error) {
	input := lastUser(req)
	hold := (f.mode == "hold" || f.mode == "two" || f.mode == "deadline") && strings.HasPrefix(input, "hold-")
	if req.Step == 1 && (hold || (f.mode == "admitted" && input == "grant-b")) {
		f.park("checkpoint", req.RunID)
	}
	return sdk.InputTokenMeasurement{Tokens: 20, Kind: sdk.MeasurementExact, ProfileVersion: "test", InputDigest: "input"}, nil
}

func lastUser(req sdk.ModelRequest) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if user := req.Messages[i].User; user != nil {
			return user.Text
		}
	}
	return ""
}

func (f restartFixture) Generate(ctx context.Context, req sdk.ModelRequest) (sdk.ModelStream, error) {
	input := lastUser(req)
	f.log(restartEvent{Kind: "model", ID: req.RunID, Input: input, Model: &req})
	call := modelCall{req, &controlledStream{ctx: ctx, events: make(chan sdk.ModelEvent, 4), closed: make(chan struct{})}}
	if input == "tool-a" && req.Step == 1 {
		call.text("old-preview")
		call.tool()
	} else {
		call.text("answer-" + input)
		call.complete("answer-" + input)
	}
	return call.stream, nil
}

type restartReceiptTool struct {
	sdk.Tool
	contract.ReceiptProvider
	fixture restartFixture
}

func (t restartReceiptTool) Invoke(ctx context.Context, req sdk.ToolRequest) (sdk.ToolResult, error) {
	result, err := t.Tool.Invoke(ctx, req)
	if err == nil && result.State == sdk.ToolExecutionKnown && t.fixture.mode == "receipt" {
		t.fixture.park("receipt", req.RunID)
	}
	return result, err
}

func (t restartReceiptTool) QueryReceipt(ctx context.Context, key contract.ReceiptKey) (*contract.ToolReceipt, error) {
	t.fixture.log(restartEvent{Kind: "query", ID: key.RunID, Key: &key})
	return t.ReceiptProvider.QueryReceipt(ctx, key)
}

type restartToolEffect struct{ fixture restartFixture }

func (t restartToolEffect) Invoke(ctx context.Context, req sdk.ToolRequest) (sdk.ToolResult, error) {
	key, ok := contract.InvocationReceiptKey(ctx)
	if !ok {
		return sdk.ToolResult{}, errors.New("expected original receipt key")
	}
	t.fixture.log(restartEvent{Kind: "tool", ID: req.RunID, Key: &key})
	return sdk.ToolResult{RunID: req.RunID, CallID: req.CallID, State: sdk.ToolExecutionKnown,
		Completion: &sdk.ToolCompletion{Outcome: sdk.ToolSucceeded, Content: "original-effect"}}, nil
}

func restartStore(root string) session.Config {
	return session.Config{Directory: filepath.Join(root, "sessions"), MaxTurns: 24, MaxHistoryBytes: 8192}
}

func restartSession(t *testing.T, root, label string) *session.Store {
	t.Helper()
	s, err := session.Open(t.Context(), restartStore(root), session.Scope{Subject: "owner", Session: label, Runner: "runner"})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (f restartFixture) checkpointPath(id sdk.RunID) string {
	key, _ := durable.Digest(struct {
		Runner  sdk.RunnerRef
		Subject sdk.SubjectID
		ID      sdk.RunID
	}{"runner", "owner", id})
	return filepath.Join(f.root, "runs", key+".run")
}

func (f restartFixture) assemble(t *testing.T) *app {
	t.Helper()
	tool, err := sdk.NewLocalReceiptTool(filepath.Join(f.root, "receipts"), restartToolEffect{f})
	if err != nil {
		t.Fatal(err)
	}
	active, binding := 1, "restart-v1"
	if f.mode == "two" {
		active = 2
	}
	if f.mode == "binding" {
		binding = "replacement"
	}
	budget := sdk.Budget{Timeout: ptr(time.Minute), MaxModelRequests: ptr(uint64(2)),
		MaxModelInputBytes: ptr(uint64(64 << 10)), MaxOutputBytes: ptr(uint64(32 << 10)), MaxToolRequests: ptr(uint64(1))}
	runner := sdk.RunnerConfig{
		AllowedModels:   map[sdk.SubjectID][]sdk.ModelRef{"owner": {"model"}},
		AllowedTools:    map[sdk.SubjectID]map[sdk.ToolRef]sdk.ToolPermission{"owner": {"slow": {}}},
		ContextPolicies: map[sdk.ModelRef]sdk.ModelContextPolicy{"model": {MaxOutputTokens: 20}},
		DefaultBudget:   budget, BudgetCaps: budget, ObserveBufferBytes: 16 << 10, ResultRetention: time.Minute,
		Recovery:   &sdk.RecoveryConfig{Directory: filepath.Join(f.root, "runs"), BindingVersion: binding, ReconcileTimeout: time.Second},
		Scheduling: &sdk.SessionSchedulingConfig{MaxActiveRuns: active, MaxQueuedRuns: 2, MaxQueueWait: 12 * time.Second},
	}
	mesh, err := sdk.NewMesh(sdk.MeshConfig{
		LocalRunners: map[sdk.RunnerRef]sdk.RunnerConfig{"runner": runner},
		LocalModels:  map[sdk.ModelRef]sdk.ModelProvider{"model": f},
		LocalTools: map[sdk.ToolRef]sdk.LocalToolBinding{"slow": {
			Tool: restartReceiptTool{tool, tool.(contract.ReceiptProvider), f},
			Spec: sdk.ToolSpec{InputSchema: json.RawMessage(`{}`), Limits: sdk.ToolLimits{MaxResultBytes: 2048}},
		}},
		ResolveCaller: func(context.Context) (sdk.SubjectID, error) { return "owner", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	client, err := sdk.NewClient(mesh, "runner")
	if err != nil {
		t.Fatal(err)
	}
	request := sdk.StartRequest{Model: "model", Tools: []sdk.ToolRef{"slow"}}
	if f.mode == "deadline" {
		// The same measurement barrier as "hold" lets the parent stop the process well before the deadline.
		request.Budget.Timeout = ptr(3 * time.Second)
	}
	a, err := open(t.Context(), Config{Client: client, Sessions: [2]sdk.SessionID{"甲", "乙"}, Store: restartStore(f.root), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	f.instrument(t, a)
	return a
}

func (f restartFixture) instrument(t *testing.T, a *app) {
	a.services.start = func(s *sdk.Session, ctx context.Context, req sdk.StartRequest) <-chan sdk.StartUpdate {
		f.log(restartEvent{Kind: "start", ID: req.ID, Input: req.Input.Text})
		if req.Input.Text == "grant-b" && (f.mode == "reserved" || f.mode == "attempted" || f.mode == "admitted") {
			return f.pauseGrant(t, s, ctx, req)
		}
		return s.StartWithProgress(ctx, req)
	}
	a.services.resume = func(s *sdk.Session, ctx context.Context, id sdk.RunID) <-chan sdk.StartUpdate {
		f.log(restartEvent{Kind: "resume", ID: id})
		return s.ResumeAdmittedWithProgress(ctx, id)
	}
	a.services.observe = func(run *sdk.Run, ctx context.Context) (sdk.EventStream, error) {
		f.log(restartEvent{Kind: "observe", ID: run.ID()})
		return run.Observe(ctx)
	}
	a.services.cancel = func(run *sdk.Run, ctx context.Context) error {
		f.log(restartEvent{Kind: "cancel", ID: run.ID()})
		err := run.Cancel(ctx)
		if err == nil {
			f.log(restartEvent{Kind: "cancel-accepted", ID: run.ID()})
		}
		return err
	}
	var saveFailed atomic.Bool
	a.services.commit = func(s *sdk.Session, ctx context.Context, id sdk.RunID) (sdk.SessionCommit, error) {
		f.log(restartEvent{Kind: "save", ID: id})
		if f.mode == "terminal" || f.mode == "sealed" {
			if f.mode == "sealed" {
				if err := f.seal(t, a, ctx, id); err != nil {
					return sdk.SessionCommit{}, err
				}
			}
			f.park("before-publish", id)
		}
		if f.mode == "save-failure" && saveFailed.CompareAndSwap(false, true) {
			f.log(restartEvent{Kind: "save-failed", ID: id})
			return sdk.SessionCommit{}, sdk.ErrSessionStorage
		}
		receipt, err := s.CommitPendingResult(ctx, id)
		if err == nil {
			f.log(restartEvent{Kind: "published", ID: id, Commit: &receipt})
		}
		return receipt, err
	}
}

func (f restartFixture) seal(t *testing.T, a *app, ctx context.Context, id sdk.RunID) error {
	store := restartSession(t, f.root, "甲")
	result, err := a.cfg.Client.Run(id).Wait(ctx)
	if err != nil {
		return err
	}
	lock, err := store.Lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	idx, err := store.Load()
	if err != nil {
		return err
	}
	_, published, err := store.PrepareCommit(idx, id, &result)
	if published {
		return errors.New("history already published at seal barrier")
	}
	return err
}

// For the two missing-checkpoint branches, install the exact C record while a real scheduler
// grant is held at the admission lock. The admitted branch pauses only the actual SDK reply.
func (f restartFixture) pauseGrant(t *testing.T, s *sdk.Session, ctx context.Context, req sdk.StartRequest) <-chan sdk.StartUpdate {
	var lock *os.File
	store := restartSession(t, f.root, "乙")
	if f.mode != "admitted" {
		var err error
		lock, err = store.AdmissionLock(ctx, req.ID)
		if err != nil {
			panic(err)
		}
	}
	updates := make(chan sdk.StartUpdate, 3)
	go func() {
		defer close(updates)
		if lock != nil {
			defer lock.Close()
		}
		for update := range s.StartWithProgress(ctx, req) {
			if lock != nil && update.Phase == sdk.StartAdmitting {
				if err := f.seedGrant(ctx, store, req); err != nil {
					panic(err)
				}
				f.park("grant-boundary", req.ID)
			}
			if f.mode == "admitted" && update.Phase == sdk.StartResolved && update.Run != nil {
				f.park("grant-boundary", req.ID)
			}
			updates <- update
		}
	}()
	return updates
}

func (f restartFixture) seedGrant(ctx context.Context, store *session.Store, req sdk.StartRequest) error {
	dir := filepath.Join(f.root, "runs")
	var authority string
	if err := durable.Read(filepath.Join(dir, ".session-authority"), &authority); err != nil {
		return err
	}
	binding, err := durable.Digest([3]string{dir, authority, "restart-v1"})
	if err != nil {
		return err
	}
	req.Recoverable = true
	digest, err := durable.Digest(req)
	if err != nil {
		return err
	}
	lock, err := store.Lock(ctx)
	if err != nil {
		return err
	}
	defer lock.Close()
	idx, err := store.Load()
	if err != nil {
		return err
	}
	idx.Pending = &session.Pending{Version: idx.Version, FirstVersion: 1, Binding: binding, RequestDigest: digest, Request: req, Attempted: f.mode == "attempted"}
	return store.Save(idx)
}

func TestTUIRestartChild(t *testing.T) {
	root := os.Getenv("SUPERAGENT_TUI_RESTART_DIR")
	if root == "" {
		return
	}
	f := restartFixture{root, os.Getenv("SUPERAGENT_TUI_RESTART_MODE")}
	if err := f.assemble(t).terminal(os.Stdin, os.Stdout); err != nil {
		t.Fatal(err)
	}
}

type restartProcess struct {
	*rig
	cmd     *exec.Cmd
	done    chan error
	drained chan struct{}
	stopped bool
}

func startRestartProcess(t *testing.T, root, mode string) *restartProcess {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTUIRestartChild$")
	cmd.Env = append(os.Environ(), "SUPERAGENT_TUI_RESTART_DIR="+root, "SUPERAGENT_TUI_RESTART_MODE="+mode)
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 32, Cols: 128})
	if err != nil {
		t.Fatal(err)
	}
	p := &restartProcess{
		rig: &rig{t: t, ctx: ctx, master: master, screens: &screenLog{notify: make(chan struct{})}},
		cmd: cmd, done: make(chan error, 1), drained: make(chan struct{}),
	}
	go func() {
		defer close(p.drained)
		p.screens.read(master)
	}()
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if !p.stopped {
			_ = cmd.Process.Kill()
			<-p.done
		}
		_ = master.Close()
	})
	return p
}

func (p *restartProcess) stop(force bool) {
	p.t.Helper()
	if force {
		if err := p.cmd.Process.Kill(); err != nil {
			p.t.Fatal(err)
		}
	} else {
		p.send("\x04")
	}
	select {
	case err := <-p.done:
		p.stopped = true
		if (err != nil) != force {
			p.t.Fatalf("process exit (forced=%v): %v", force, err)
		}
	case <-time.After(2 * time.Second):
		p.t.Fatal("terminal did not exit in time")
	}
	recv(p.t, p.ctx, p.drained)
}

func restartAudit(t *testing.T, root, kind string, id sdk.RunID) []restartEvent {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "audit"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	// A last incomplete row can only belong to the concurrently writing process.
	var events []restartEvent
	for _, line := range bytes.Split(data[:bytes.LastIndexByte(data, '\n')+1], []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var event restartEvent
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		if event.Kind == kind && (id == "" || event.ID == id) {
			events = append(events, event)
		}
	}
	return events
}

func awaitRestartEvent(t *testing.T, root, kind string, id sdk.RunID) restartEvent {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if found := restartAudit(t, root, kind, id); len(found) != 0 {
			return found[len(found)-1]
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("missing %s for %s", kind, id)
	return restartEvent{}
}

func checkRestartCount(t *testing.T, root, kind string, id sdk.RunID, want int) {
	t.Helper()
	if events := restartAudit(t, root, kind, id); len(events) != want {
		t.Fatalf("%s (%s): got %d, want %d (%+v)", kind, id, len(events), want, events)
	}
}

func checkRestartIndex(t *testing.T, root, label string, version uint64, pending sdk.RunID) session.Index {
	t.Helper()
	idx, err := restartSession(t, root, label).Load()
	if err != nil || idx.Version != version || (idx.Pending != nil) != (pending != "") ||
		(idx.Pending != nil && idx.Pending.Request.ID != pending) {
		t.Fatalf("session %s: expected version %d with pending %q, got %+v, %v", label, version, pending, idx, err)
	}
	return idx
}
