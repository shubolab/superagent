//go:build darwin || linux

package tui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
	sdk "github.com/shubolab/superagent"
)

type controlledModel struct{ calls chan modelCall }
type modelCall struct {
	request sdk.ModelRequest
	stream  *controlledStream
}

func (m controlledModel) Generate(ctx context.Context, req sdk.ModelRequest) (sdk.ModelStream, error) {
	stream := &controlledStream{ctx: ctx, events: make(chan sdk.ModelEvent, 8), closed: make(chan struct{})}
	select {
	case m.calls <- modelCall{req, stream}:
		return stream, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type controlledStream struct {
	ctx    context.Context
	events chan sdk.ModelEvent
	closed chan struct{}
	once   sync.Once
}

func (s *controlledStream) Recv() (sdk.ModelEvent, error) {
	select {
	case <-s.ctx.Done():
		return sdk.ModelEvent{}, s.ctx.Err()
	case <-s.closed:
		return sdk.ModelEvent{}, io.EOF
	case event := <-s.events:
		return event, nil
	}
}

func (s *controlledStream) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

func (c modelCall) text(text string) {
	c.stream.events <- sdk.ModelEvent{Kind: sdk.ModelTextDelta, TextDelta: text}
}

func (c modelCall) complete(text string) {
	c.stream.events <- sdk.ModelEvent{Kind: sdk.ModelCompleted, Reply: &sdk.ModelReply{Finish: sdk.ModelFinishStop, Text: text}}
}

func (c modelCall) tool() {
	c.stream.events <- sdk.ModelEvent{Kind: sdk.ModelCompleted, Reply: &sdk.ModelReply{
		Finish:    sdk.ModelFinishToolCalls,
		ToolCalls: []sdk.ModelToolCall{{CallID: "c1", Tool: "slow", Arguments: json.RawMessage(`{}`)}},
	}}
}

type controlledTool struct {
	entered chan sdk.RunID
	release chan struct{}
}

func (tool controlledTool) Invoke(ctx context.Context, req sdk.ToolRequest) (sdk.ToolResult, error) {
	select {
	case tool.entered <- req.RunID:
	case <-ctx.Done():
		return sdk.ToolResult{}, ctx.Err()
	}
	select {
	case <-tool.release:
		return sdk.ToolResult{RunID: req.RunID, CallID: req.CallID, State: sdk.ToolExecutionKnown,
			Completion: &sdk.ToolCompletion{Outcome: sdk.ToolSucceeded, Content: "released"}}, nil
	case <-ctx.Done():
		return sdk.ToolResult{RunID: req.RunID, CallID: req.CallID, State: sdk.ToolExecutionNotExecuted,
			Reason: &sdk.ToolReason{Code: "cancelled"}}, nil
	}
}

type activity struct {
	mu     sync.Mutex
	counts map[string]map[sdk.RunID]int
}

func (a *activity) record(kind string, id sdk.RunID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.counts[kind] == nil {
		a.counts[kind] = make(map[sdk.RunID]int)
	}
	a.counts[kind][id]++
}

func (a *activity) count(kind string, id sdk.RunID) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.counts[kind][id]
}

type rig struct {
	t       *testing.T
	ctx     context.Context
	app     *app
	calls   chan modelCall
	tool    controlledTool
	audit   *activity
	screens *screenLog
	master  *os.File
	next    int
}

func newRig(t *testing.T, active, queued int, queueWait time.Duration) *rig {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	t.Cleanup(cancel)
	root := t.TempDir()
	calls := make(chan modelCall, 24)
	slow := controlledTool{entered: make(chan sdk.RunID, 4), release: make(chan struct{}, 4)}
	tool, err := sdk.NewLocalReceiptTool(filepath.Join(root, "receipts"), slow)
	if err != nil {
		t.Fatal(err)
	}
	budget := sdk.Budget{
		Timeout: ptr(10 * time.Second), MaxModelRequests: ptr(uint64(4)),
		MaxModelInputBytes: ptr(uint64(64 << 10)), MaxOutputBytes: ptr(uint64(32 << 10)), MaxToolRequests: ptr(uint64(2)),
	}
	mesh, err := sdk.NewMesh(sdk.MeshConfig{
		LocalRunners: map[sdk.RunnerRef]sdk.RunnerConfig{"runner": {
			AllowedModels: map[sdk.SubjectID][]sdk.ModelRef{"owner": {"model"}},
			AllowedTools:  map[sdk.SubjectID]map[sdk.ToolRef]sdk.ToolPermission{"owner": {"slow": {}}},
			DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 16 << 10, ResultRetention: time.Minute,
			Recovery:   &sdk.RecoveryConfig{Directory: filepath.Join(root, "runs"), BindingVersion: "pty-test", ReconcileTimeout: 100 * time.Millisecond},
			Scheduling: &sdk.SessionSchedulingConfig{MaxActiveRuns: active, MaxQueuedRuns: queued, MaxQueueWait: queueWait},
		}},
		LocalModels: map[sdk.ModelRef]sdk.ModelProvider{"model": controlledModel{calls}},
		LocalTools: map[sdk.ToolRef]sdk.LocalToolBinding{"slow": {
			Tool: tool, Spec: sdk.ToolSpec{InputSchema: json.RawMessage(`{}`), Limits: sdk.ToolLimits{MaxResultBytes: 2048}},
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
	a, err := open(ctx, Config{
		Client: client, Sessions: [2]sdk.SessionID{"甲", "乙"},
		Store:   sdk.SessionConfig{Directory: filepath.Join(root, "sessions"), MaxTurns: 24, MaxHistoryBytes: 8192},
		Request: sdk.StartRequest{Model: "model", Tools: []sdk.ToolRef{"slow"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	audit := &activity{counts: make(map[string]map[sdk.RunID]int)}
	a.services.start = func(s *sdk.Session, ctx context.Context, req sdk.StartRequest) <-chan sdk.StartUpdate {
		audit.record("start", req.ID)
		return s.StartWithProgress(ctx, req)
	}
	a.services.observe = func(run *sdk.Run, ctx context.Context) (sdk.EventStream, error) {
		audit.record("observe", run.ID())
		return run.Observe(ctx)
	}
	a.services.cancel = func(run *sdk.Run, ctx context.Context) error {
		audit.record("cancel", run.ID())
		return run.Cancel(ctx)
	}
	a.services.commit = func(s *sdk.Session, ctx context.Context, id sdk.RunID) (sdk.SessionCommit, error) {
		audit.record("commit", id)
		return s.CommitRun(ctx, id)
	}
	return &rig{t: t, ctx: ctx, app: a, calls: calls, tool: slow, audit: audit}
}

func ptr[T any](value T) *T { return &value }

const frameStart = "\x1b[H\x1b[2J"
const frameEnd = "Ctrl+D 退出"

type screenLog struct {
	mu     sync.Mutex
	frames []string
	notify chan struct{}
}

func (l *screenLog) read(master io.Reader) {
	var chunk [4096]byte
	var pending string
	for {
		n, err := master.Read(chunk[:])
		pending += string(chunk[:n])
		for {
			start := strings.Index(pending, frameStart)
			if start < 0 {
				pending = pending[max(0, len(pending)-len(frameStart)+1):]
				break
			}
			pending = pending[start:]
			end := strings.Index(pending, frameEnd)
			if end < 0 {
				break
			}
			end += len(frameEnd)
			l.mu.Lock()
			l.frames = append(l.frames, pending[len(frameStart):end])
			close(l.notify)
			l.notify = make(chan struct{})
			l.mu.Unlock()
			pending = pending[end:]
		}
		if err != nil {
			return
		}
	}
}

func (r *rig) launch() {
	r.t.Helper()
	master, slave, err := pty.Open()
	if err != nil {
		r.t.Fatal(err)
	}
	if err := pty.Setsize(master, &pty.Winsize{Rows: 32, Cols: 100}); err != nil {
		_ = master.Close()
		_ = slave.Close()
		r.t.Fatal(err)
	}
	r.master = master
	r.screens = &screenLog{notify: make(chan struct{})}
	go r.screens.read(master)
	done := make(chan error, 1)
	go func() { done <- r.app.terminal(slave, slave) }()
	r.t.Cleanup(func() {
		_, _ = master.Write([]byte{'\x04'})
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, os.ErrClosed) {
				r.t.Error(err)
			}
		case <-time.After(time.Second):
			r.t.Error("PTY application did not exit")
		}
		_ = master.Close()
		_ = slave.Close()
	})
	r.expect("[甲 · 空闲]", "乙 · 空闲")
}

func (r *rig) send(keys string) {
	r.t.Helper()
	if _, err := r.master.Write([]byte(keys)); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) expect(parts ...string) string {
	r.t.Helper()
	for {
		log := r.screens
		log.mu.Lock()
		var latest string
		for ; r.next < len(log.frames); r.next++ {
			frame := log.frames[r.next]
			match := true
			for _, part := range parts {
				match = match && strings.Contains(frame, part)
			}
			if match {
				r.next++
				log.mu.Unlock()
				return frame
			}
		}
		if len(log.frames) > 0 {
			latest = log.frames[len(log.frames)-1]
		}
		notify := log.notify
		log.mu.Unlock()
		select {
		case <-notify:
		case <-r.ctx.Done():
			r.t.Fatalf("PTY did not show %q. Latest screen:\n%s", parts, latest)
		}
	}
}

func recv[T any](t *testing.T, ctx context.Context, c <-chan T) T {
	t.Helper()
	select {
	case value := <-c:
		return value
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var value T
		return value
	}
}

func (r *rig) called(input string, step uint32) modelCall {
	r.t.Helper()
	call := recv(r.t, r.ctx, r.calls)
	var latest string
	for _, message := range call.request.Messages {
		if message.User != nil {
			latest = message.User.Text
		}
	}
	if latest != input || call.request.Step != step {
		r.t.Fatalf("dispatch = %q step %d, want %q step %d", latest, call.request.Step, input, step)
	}
	return call
}

func (r *rig) holdA() modelCall {
	r.t.Helper()
	r.send("slow-a\r")
	call := r.called("slow-a", 1)
	call.tool()
	if id := recv(r.t, r.ctx, r.tool.entered); id != call.request.RunID {
		r.t.Fatal(id)
	}
	r.expect("[甲 · 运行中]", "工具 · slow")
	return call
}

func (r *rig) finishA() {
	r.t.Helper()
	r.tool.release <- struct{}{}
	r.called("slow-a", 2).complete("a-done")
}

func (r *rig) saved(index int, version uint64) {
	r.t.Helper()
	snapshot, err := r.app.views[index].session.Snapshot(r.ctx)
	if err != nil || snapshot.Version != version {
		r.t.Fatalf("session %d version = %d, want %d (%v)", index, snapshot.Version, version, err)
	}
}

func (r *rig) noDispatch() {
	r.t.Helper()
	select {
	case call := <-r.calls:
		r.t.Fatalf("unexpected dispatch for %s step %d", call.request.RunID, call.request.Step)
	default:
	}
}
