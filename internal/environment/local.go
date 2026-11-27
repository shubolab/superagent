// Package environment owns local workspace roots and per-call read grants.
package environment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type WorkspaceConfig struct {
	Root         string
	ReadPrefixes []string
}

type workspace struct {
	path     string
	identity fs.FileInfo
	prefixes map[string]fs.FileInfo
}

type allocation struct {
	scope  contract.RunScope
	root   *os.Root
	grants map[contract.EnvironmentGrantRef]*grant
}

type grant struct {
	scope  contract.CallScope
	root   *os.Root
	file   string
	reader *readHandle
}

type Local struct {
	workspaces map[contract.WorkspaceRef]workspace
	mu         sync.Mutex
	next       uint64
	runs       map[contract.EnvironmentHandle]*allocation
}

func NewLocal(config map[contract.WorkspaceRef]WorkspaceConfig) (*Local, error) {
	if !supportsRootedRead || len(config) == 0 {
		return nil, fmt.Errorf("%w: local rooted read environment unavailable", contract.ErrInvalidConfiguration)
	}
	local := &Local{
		workspaces: make(map[contract.WorkspaceRef]workspace, len(config)),
		runs:       make(map[contract.EnvironmentHandle]*allocation),
	}
	for ref, cfg := range config {
		if ref == "" || !filepath.IsAbs(cfg.Root) || len(cfg.ReadPrefixes) == 0 {
			return nil, fmt.Errorf("%w: invalid workspace %q", contract.ErrInvalidConfiguration, ref)
		}
		ws, err := inspectWorkspace(cfg)
		if err != nil {
			return nil, fmt.Errorf("%w: workspace %q: %v", contract.ErrInvalidConfiguration, ref, err)
		}
		local.workspaces[ref] = ws
	}
	return local, nil
}

func inspectWorkspace(cfg WorkspaceConfig) (workspace, error) {
	root, err := os.OpenRoot(cfg.Root)
	if err != nil {
		return workspace{}, err
	}
	defer root.Close()
	identity, err := root.Stat(".")
	if err != nil {
		return workspace{}, err
	}
	prefixes := make(map[string]fs.FileInfo, len(cfg.ReadPrefixes))
	for _, prefix := range cfg.ReadPrefixes {
		if !fs.ValidPath(prefix) || strings.Contains(prefix, "\\") {
			return workspace{}, contract.ErrInvalidRequest
		}
		info, err := root.Lstat(prefix)
		if err != nil || !info.IsDir() {
			return workspace{}, contract.ErrAccessDenied
		}
		prefixes[prefix] = info
	}
	return workspace{path: cfg.Root, identity: identity, prefixes: prefixes}, nil
}

func (l *Local) Allocate(ctx context.Context, scope contract.RunScope) (contract.EnvironmentHandle, error) {
	ws, ok := l.workspaces[scope.Workspace]
	if !ok || scope.Runner == "" || scope.Subject == "" || scope.RunID == "" || expired(ctx, scope) {
		return "", contract.ErrAccessDenied
	}
	root, err := os.OpenRoot(ws.path)
	if err != nil {
		return "", err
	}
	info, err := root.Stat(".")
	if err != nil || !os.SameFile(ws.identity, info) {
		_ = root.Close()
		return "", contract.ErrAccessDenied
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.next++
	handle := contract.EnvironmentHandle(strconv.FormatUint(l.next, 10))
	l.runs[handle] = &allocation{scope: scope, root: root, grants: make(map[contract.EnvironmentGrantRef]*grant)}
	return handle, nil
}

func RelativeFile(prefix, file string) (string, bool) {
	if file == "." || !fs.ValidPath(file) || strings.Contains(file, "\\") {
		return "", false
	}
	if prefix == "." {
		return file, true
	}
	return strings.CutPrefix(file, prefix+"/")
}

func (l *Local) Grant(ctx context.Context, scope contract.CallScope, prefix string) (contract.EnvironmentGrantRef, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	run := l.runs[scope.Handle]
	ws := l.workspaces[scope.Run.Workspace]
	identity, allowed := ws.prefixes[prefix]
	file, valid := RelativeFile(prefix, scope.Path)
	if run == nil || run.scope != scope.Run || scope.CallID == "" || scope.Tool == "" ||
		scope.Limits.MaxScanBytes == 0 || scope.Limits.MaxResultBytes == 0 || !allowed || !valid || expired(ctx, scope.Run) {
		return "", contract.ErrAccessDenied
	}
	for _, g := range run.grants {
		if g.scope.CallID == scope.CallID {
			return "", contract.ErrAccessDenied
		}
	}
	root, err := run.root.OpenRoot(prefix)
	if err != nil {
		return "", err
	}
	info, err := root.Stat(".")
	if err != nil || !os.SameFile(identity, info) {
		_ = root.Close()
		return "", contract.ErrAccessDenied
	}
	l.next++
	ref := contract.EnvironmentGrantRef(strconv.FormatUint(l.next, 10))
	run.grants[ref] = &grant{scope: scope, root: root, file: file}
	return ref, nil
}

func (l *Local) OpenRead(ctx context.Context, scope contract.CallScope, ref contract.EnvironmentGrantRef, file string) (io.ReadCloser, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, g := l.findGrant(scope, ref)
	if g == nil || g.reader != nil || scope.Path != file || expired(ctx, scope.Run) {
		return nil, contract.ErrAccessDenied
	}
	info, err := g.root.Stat(g.file)
	if err != nil || !info.Mode().IsRegular() {
		return nil, contract.ErrAccessDenied
	}
	f, err := openNonblocking(g.root, g.file)
	if err != nil {
		return nil, err
	}
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, contract.ErrAccessDenied
	}
	readCtx, cancel := context.WithDeadline(ctx, scope.Run.Deadline)
	r := &readHandle{
		f: f, ctx: readCtx, cancel: cancel, limit: scope.Limits.MaxScanBytes,
		closeFile: sync.OnceValue(f.Close),
	}
	r.stop = context.AfterFunc(readCtx, func() { _ = r.closeFile() })
	g.reader = r
	return r, nil
}

func (l *Local) Revoke(ctx context.Context, scope contract.CallScope, ref contract.EnvironmentGrantRef) (uint64, error) {
	l.mu.Lock()
	run, g := l.findGrant(scope, ref)
	if g == nil || ctx.Err() != nil {
		l.mu.Unlock()
		return 0, contract.ErrAccessDenied
	}
	delete(run.grants, ref)
	l.mu.Unlock()
	return g.close()
}

func (l *Local) Release(ctx context.Context, scope contract.RunScope, handle contract.EnvironmentHandle) error {
	l.mu.Lock()
	run := l.runs[handle]
	if run == nil || run.scope != scope || ctx.Err() != nil {
		l.mu.Unlock()
		return contract.ErrAccessDenied
	}
	delete(l.runs, handle)
	l.mu.Unlock()
	var err error
	for _, g := range run.grants {
		_, closeErr := g.close()
		err = errors.Join(err, closeErr)
	}
	return errors.Join(err, run.root.Close())
}

func (l *Local) findGrant(scope contract.CallScope, ref contract.EnvironmentGrantRef) (*allocation, *grant) {
	run := l.runs[scope.Handle]
	if run == nil || run.scope != scope.Run {
		return nil, nil
	}
	g := run.grants[ref]
	if g == nil || g.scope != scope {
		return nil, nil
	}
	return run, g
}

func expired(ctx context.Context, scope contract.RunScope) bool {
	return ctx.Err() != nil || !time.Now().Before(scope.Deadline)
}

func (g *grant) close() (uint64, error) {
	var scanned uint64
	if g.reader != nil {
		err := g.reader.Close()
		g.reader.mu.Lock()
		scanned = g.reader.used.Load()
		g.reader.mu.Unlock()
		return scanned, errors.Join(err, g.root.Close())
	}
	return scanned, g.root.Close()
}

type readHandle struct {
	f         *os.File
	ctx       context.Context
	cancel    context.CancelFunc
	stop      func() bool
	closeFile func() error
	limit     uint64
	used      atomic.Uint64
	mu        sync.Mutex
}

func (r *readHandle) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	remaining := r.limit - r.used.Load()
	if remaining == 0 {
		return 0, contract.ErrReadLimit
	}
	n, err := r.f.Read(p[:min(uint64(len(p)), remaining)])
	r.used.Add(uint64(n))
	return n, err
}

func (r *readHandle) Close() error {
	r.stop()
	r.cancel()
	return r.closeFile()
}

func (l *Local) AllowsPrefix(workspace contract.WorkspaceRef, prefix string) bool {
	_, ok := l.workspaces[workspace].prefixes[prefix]
	return ok
}
