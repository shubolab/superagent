//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	sdk "github.com/shubolab/superagent"
	"github.com/shubolab/superagent/internal/durable"
	"github.com/shubolab/superagent/internal/session"
)

const frameStart = "\x1b[H\x1b[2J"
const frameEnd = "Ctrl+D 退出"

type cliProcess struct {
	t       *testing.T
	ctx     context.Context
	cmd     *exec.Cmd
	pty     *os.File
	done    chan error
	drained chan struct{}
	stopped bool

	mu     sync.Mutex
	notify chan struct{}
	frames []string
	next   int
	tail   string
}

func (p *cliProcess) collect() {
	defer close(p.drained)
	var pending string
	chunk := make([]byte, 4096)
	for {
		n, err := p.pty.Read(chunk)
		p.mu.Lock()
		p.tail += string(chunk[:n])
		p.tail = p.tail[max(0, len(p.tail)-4096):]
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
			p.frames = append(p.frames, pending[len(frameStart):end])
			pending = pending[end:]
		}
		close(p.notify)
		p.notify = make(chan struct{})
		p.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (p *cliProcess) send(keys string) {
	p.t.Helper()
	if _, err := p.pty.WriteString(keys); err != nil {
		p.t.Fatal(err)
	}
}

func (p *cliProcess) expect(parts ...string) string {
	p.t.Helper()
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	for {
		p.mu.Lock()
		for ; p.next < len(p.frames); p.next++ {
			frame := p.frames[p.next]
			match := true
			for _, part := range parts {
				match = match && strings.Contains(frame, part)
			}
			if match {
				p.next++
				p.mu.Unlock()
				return frame
			}
		}
		notify, tail := p.notify, p.tail
		p.mu.Unlock()
		select {
		case <-notify:
		case <-p.drained:
			p.t.Fatalf("command exited before showing %q. Terminal:\n%s", parts, tail)
		case <-timer.C:
			p.t.Fatalf("terminal did not show %q. Terminal:\n%s", parts, tail)
		}
	}
}

func (p *cliProcess) ready() {
	p.t.Helper()
	p.expect("[甲 · 空闲]", "乙 · 空闲")
}

func (p *cliProcess) stop(force bool) {
	p.t.Helper()
	if force {
		if err := p.cmd.Process.Kill(); err != nil {
			p.t.Fatal(err)
		}
	} else {
		p.send("\x04")
	}
	err := p.exited()
	if (err != nil) != force {
		p.t.Fatalf("command exit (forced=%v): %v; %s", force, err, p.tail)
	}
}

func (p *cliProcess) exited() error {
	p.t.Helper()
	select {
	case err := <-p.done:
		p.stopped = true
		select {
		case <-p.drained:
			return err
		case <-p.ctx.Done():
			p.t.Fatal("PTY reader did not drain")
		}
	case <-time.After(4 * time.Second):
		p.t.Fatal("command did not exit")
	}
	return nil
}

func (p *cliProcess) peakRSS() int64 {
	p.t.Helper()
	usage, ok := p.cmd.ProcessState.SysUsage().(*syscall.Rusage)
	if !ok || usage.Maxrss <= 0 {
		p.t.Fatal("process peak RSS unavailable")
	}
	value := int64(usage.Maxrss)
	if runtime.GOOS == "linux" {
		value *= 1024
	}
	return value
}

type searchCase struct {
	t         *testing.T
	path      string
	root      string
	workspace string
	state     string
	wire      *wireModel
}

func newSearchCase(t *testing.T, binary string) *searchCase {
	t.Helper()
	root := t.TempDir()
	s := &searchCase{t: t, path: binary, root: root, workspace: filepath.Join(root, "workspace"), state: filepath.Join(root, "state")}
	if err := os.MkdirAll(filepath.Join(s.workspace, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	s.file("docs/health.txt", "status=ready\n")
	s.wire = newWireModel(t)
	return s
}

func (s *searchCase) file(name, content string) {
	s.t.Helper()
	if err := os.WriteFile(filepath.Join(s.workspace, name), []byte(content), 0600); err != nil {
		s.t.Fatal(err)
	}
}

func (s *searchCase) start(workspace, fault string) *cliProcess {
	s.t.Helper()
	args := []string{"-model", "loopback-search", "-state-dir", s.state, "-parallel", "2"}
	if workspace != "" {
		args = append(args, "-workspace", workspace)
	}
	return s.startArgs(fault, args)
}

func (s *searchCase) startArgs(fault string, args []string) *cliProcess {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.t.Context(), 25*time.Second)
	s.t.Cleanup(cancel)
	binary := s.path
	env := append(os.Environ(), "ANTHROPIC_AUTH_TOKEN=", "ANTHROPIC_API_KEY=loopback-only", "ANTHROPIC_BASE_URL="+s.wire.server.URL)
	if fault != "" {
		binary = os.Args[0]
		args = append([]string{"-test.run=^TestSearchCommandChild$", "--"}, args...)
		env = append(env, "SUPERAGENT_SEARCH_TEST="+fault, "SUPERAGENT_SEARCH_AUDIT="+s.root)
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	master, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 36, Cols: 168})
	if err != nil {
		s.t.Fatal(err)
	}
	p := &cliProcess{t: s.t, ctx: ctx, cmd: cmd, pty: master,
		done: make(chan error, 1), drained: make(chan struct{}), notify: make(chan struct{})}
	go p.collect()
	go func() { p.done <- cmd.Wait() }()
	s.t.Cleanup(func() {
		if !p.stopped {
			_ = cmd.Process.Kill()
			<-p.done
		}
		_ = master.Close()
	})
	return p
}

func (s *searchCase) store(label string) *session.Store {
	s.t.Helper()
	store, err := session.Open(s.t.Context(), sdk.SessionConfig{
		Directory: filepath.Join(s.state, "sessions"), MaxTurns: 512, MaxHistoryBytes: 128 << 10,
	}, session.Scope{Subject: "terminal", Session: label, Runner: "local"})
	if err != nil {
		s.t.Fatal(err)
	}
	return store
}

func (s *searchCase) index(label string, version uint64, pending sdk.RunID) session.Index {
	s.t.Helper()
	idx, err := s.store(label).Load()
	if err != nil || idx.Version != version || (idx.Pending != nil) != (pending != "") ||
		(idx.Pending != nil && idx.Pending.Request.ID != pending) {
		s.t.Fatalf("%s expected v%d with pending %q: %+v %v", label, version, pending, idx, err)
	}
	return idx
}

func (s *searchCase) receipt(call sdk.CallID) (string, journalEntry) {
	s.t.Helper()
	files, err := filepath.Glob(filepath.Join(s.state, "search-receipts", "*.receipt"))
	if err != nil {
		s.t.Fatal(err)
	}
	for _, file := range files {
		var record journalEntry
		if err := durable.Read(file, &record); err != nil {
			s.t.Fatal(err)
		}
		if strings.HasSuffix(string(record.Key.CallID), ":"+string(call)) {
			return file, record
		}
	}
	s.t.Fatalf("missing journal entry for %s (records %v)", call, files)
	return "", journalEntry{}
}

type journalEntry struct {
	Key     sdk.ReceiptKey
	Result  *sdk.ToolResult
	Cleanup sdk.CleanupStatus
	Scan    *durable.ScanMeasurement `json:",omitempty"`
}

func (s *searchCase) checkpoint(id sdk.RunID) runRecord {
	s.t.Helper()
	key, err := durable.Digest(struct {
		Runner  sdk.RunnerRef
		Subject sdk.SubjectID
		ID      sdk.RunID
	}{"local", "terminal", id})
	if err != nil {
		s.t.Fatal(err)
	}
	var record runRecord
	data, err := os.ReadFile(filepath.Join(s.state, "runs", key+".run"))
	if err == nil {
		err = json.Unmarshal(data, &record)
	}
	if err != nil {
		s.t.Fatal(err)
	}
	return record
}

type runRecord struct {
	Step             uint32
	Deadline         time.Time
	ReconcileUntil   time.Time
	ReconcileTimeout time.Duration
	ModelInFlight    bool
	Intent           *struct {
		Key    sdk.ReceiptKey
		Limits sdk.ToolLimits
	}
	Balance  struct{ Scan, ToolRequests, Output uint64 }
	Result   *sdk.Result
	Messages []sdk.Message
}

func (s *searchCase) terminalCheckpoint(id sdk.RunID) runRecord {
	s.t.Helper()
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline); {
		record := s.checkpoint(id)
		if record.Result != nil {
			return record
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.t.Fatalf("no durable terminal for %s", id)
	return runRecord{}
}

func builtCLI(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "superagent")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", path, "./cmd/superagent")
	cmd.Dir = "../.."
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, output)
	}
	return path
}

func rejectFrame(t *testing.T, frame, part string) {
	t.Helper()
	if strings.Contains(frame, part) {
		t.Fatalf("unexpected %q in frame:\n%s", part, frame)
	}
}

func verifyRun(t *testing.T, record runRecord, id sdk.RunID, outcome sdk.RunOutcome, step uint32) {
	t.Helper()
	if record.Result == nil || record.Result.RunID != id || record.Result.Outcome != outcome || record.Step != step {
		t.Fatalf("expected %s at step %d for %s, got %+v", outcome, step, id, record)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Audit lines are appended atomically by independent children.
func appendProbe(dir string, probe searchProbe) {
	data, err := json.Marshal(probe)
	if err != nil {
		panic(err)
	}
	file, err := os.OpenFile(filepath.Join(dir, "audit"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		panic(err)
	}
	_, err = file.Write(append(data, '\n'))
	if err := errors.Join(err, file.Sync(), file.Close()); err != nil {
		panic(fmt.Errorf("append audit: %w", err))
	}
}
