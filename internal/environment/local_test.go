//go:build darwin || linux

package environment

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func TestWorkspaceReadGrant(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"docs/health.txt", "secret.txt"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("status=ready\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("../secret.txt", filepath.Join(dir, "docs", "escape")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(dir, "docs", "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := NewLocal(map[contract.WorkspaceRef]WorkspaceConfig{"w": {Root: dir, ReadPrefixes: []string{"docs"}}})
	if err != nil {
		t.Fatal(err)
	}
	run := contract.RunScope{Runner: "r", Subject: "alice", RunID: "id", Workspace: "w", Deadline: time.Now().Add(time.Minute)}
	h, err := l.Allocate(ctx, run)
	if err != nil {
		t.Fatal(err)
	}
	call := contract.CallScope{Run: run, CallID: "c1", Tool: "search", Handle: h, Path: "docs/health.txt",
		Limits: contract.ToolLimits{MaxScanBytes: 5, MaxResultBytes: 100}}
	grant, err := l.Grant(ctx, call, "docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*contract.CallScope){
		func(c *contract.CallScope) { c.Run.Subject = "bob" },
		func(c *contract.CallScope) { c.Run.Runner = "other" },
		func(c *contract.CallScope) { c.Run.RunID = "other" },
		func(c *contract.CallScope) { c.Run.Workspace = "other" },
		func(c *contract.CallScope) { c.CallID = "c2" },
		func(c *contract.CallScope) { c.Tool = "other" },
		func(c *contract.CallScope) { c.Limits.MaxScanBytes++ },
		func(c *contract.CallScope) { c.Run.Deadline = time.Now().Add(time.Hour) },
	} {
		foreign := call
		change(&foreign)
		if r, err := l.OpenRead(ctx, foreign, grant, call.Path); r != nil || !errors.Is(err, contract.ErrAccessDenied) {
			t.Fatalf("foreign grant read = %v, %v, scope = %+v", r, err, foreign)
		}
	}
	if _, err := l.OpenRead(ctx, call, grant, "docs/other"); !errors.Is(err, contract.ErrAccessDenied) {
		t.Fatalf("wrong path = %v", err)
	}
	for _, file := range []string{"/etc/passwd", "docs/../secret.txt", "secret.txt", "docs", "docs/escape", "docs/fifo"} {
		bad := call
		bad.CallID, bad.Path = "bad", file
		ref, err := l.Grant(ctx, bad, "docs")
		if err != nil {
			continue
		}
		if reader, err := l.OpenRead(ctx, bad, ref, file); reader != nil || err == nil {
			t.Fatalf("unexpected open for %s = %v, %v", file, reader, err)
		}
		if scanned, err := l.Revoke(ctx, bad, ref); err != nil || scanned != 0 {
			t.Fatalf("invalid file meter = %d, %v", scanned, err)
		}
	}
	r, err := l.OpenRead(ctx, call, grant, call.Path)
	if err != nil {
		t.Fatal(err)
	}
	if p, err := io.ReadAll(r); string(p) != "statu" || !errors.Is(err, contract.ErrReadLimit) {
		t.Fatalf("bounded read = %q, %v", p, err)
	}
	cancel()
	if n, err := r.Read(make([]byte, 1)); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled read = %d, %v", n, err)
	}
	if scanned, err := l.Revoke(t.Context(), call, grant); scanned != 5 || err != nil {
		t.Fatalf("revoke measured %d, %v", scanned, err)
	}
	if _, err := r.(*readHandle).f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("file was not closed: %v", err)
	}
	if _, err := l.OpenRead(t.Context(), call, grant, call.Path); !errors.Is(err, contract.ErrAccessDenied) {
		t.Fatalf("revoked grant = %v", err)
	}
	if err := l.Release(t.Context(), run, h); err != nil || len(l.runs) != 0 {
		t.Fatalf("release = %v, allocations = %d", err, len(l.runs))
	}

	expired := run
	expired.Deadline = time.Now().Add(time.Second)
	h, err = l.Allocate(t.Context(), expired)
	if err != nil {
		t.Fatal(err)
	}
	call.Run, call.Handle = expired, h
	grant, err = l.Grant(t.Context(), call, "docs")
	if err != nil {
		t.Fatal(err)
	}
	deadline, done := context.WithDeadline(t.Context(), expired.Deadline)
	defer done()
	<-deadline.Done()
	if _, err := l.OpenRead(t.Context(), call, grant, call.Path); !errors.Is(err, contract.ErrAccessDenied) {
		t.Fatalf("expired grant = %v", err)
	}
	if n, err := l.Revoke(t.Context(), call, grant); n != 0 || err != nil {
		t.Fatalf("expired grant measured %d, %v", n, err)
	}
	if err := l.Release(t.Context(), expired, h); err != nil {
		t.Fatal(err)
	}
}
