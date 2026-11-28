// Package artifact contains the local managed output store.
package artifact

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

type Config struct {
	Directory        string
	MaxArtifactBytes uint64
	MaxStoredBytes   uint64
	MaxArtifacts     int
	MaxPageBytes     uint64
	Retention        time.Duration
}

type Local struct {
	mu       sync.Mutex
	cfg      Config
	path     string
	closed   bool
	reserved uint64
	calls    map[callID]*item
	files    map[string]*item
}

type callID struct {
	owner contract.ArtifactOwner
	call  contract.CallID
}

type item struct {
	id      string
	scope   contract.ArtifactCall
	path    string
	file    *os.File
	hash    hash.Hash
	used    uint64
	failed  bool
	staged  contract.ArtifactRef
	expiry  *time.Timer
	created bool
}

func NewLocal(cfg Config) (*Local, error) {
	if !validConfig(cfg) {
		return nil, contract.ErrInvalidConfiguration
	}
	dir, err := os.MkdirTemp(cfg.Directory, "superagent-artifacts-")
	if err != nil {
		return nil, err
	}
	return &Local{cfg: cfg, path: dir, calls: make(map[callID]*item), files: make(map[string]*item)}, nil
}

func validConfig(cfg Config) bool {
	return filepath.IsAbs(cfg.Directory) && cfg.MaxArtifactBytes > 0 && cfg.MaxStoredBytes >= cfg.MaxArtifactBytes &&
		cfg.MaxArtifactBytes <= math.MaxInt64 && cfg.MaxPageBytes > 0 && cfg.MaxPageBytes <= contract.ArtifactMaxBlockBytes &&
		cfg.MaxArtifacts > 0 && cfg.Retention > 0
}

func (s *Local) Begin(ctx context.Context, scope contract.ArtifactCall) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := callID{owner: scope.Owner, call: scope.CallID}
	if s.closed || key.owner.RunID == "" || key.owner.Runner == "" || key.owner.Subject == "" ||
		key.call == "" || scope.Tool == "" || scope.MaxBytes == 0 {
		return 0, contract.ErrAccessDenied
	}
	if _, exists := s.calls[key]; exists {
		return 0, contract.ErrInvalidRequest
	}
	capacity := min(scope.MaxBytes, s.cfg.MaxArtifactBytes, s.cfg.MaxStoredBytes-s.reserved)
	if capacity == 0 || len(s.calls)+len(s.files) >= s.cfg.MaxArtifacts {
		return 0, contract.ErrArtifactCapacity
	}
	scope.MaxBytes = capacity
	id := rand.Text()
	s.calls[key] = &item{id: id, scope: scope, path: filepath.Join(s.path, id)}
	s.reserved += capacity
	return capacity, nil
}

func (s *Local) lookup(scope contract.ArtifactCall) *item {
	entry := s.calls[callID{owner: scope.Owner, call: scope.CallID}]
	if entry == nil || entry.scope != scope {
		return nil
	}
	return entry
}

func (s *Local) OpenWriter(ctx context.Context, scope contract.ArtifactCall) (contract.ArtifactWriter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.lookup(scope)
	if e == nil || e.file != nil || e.staged.ID != "" || e.failed {
		return nil, contract.ErrArtifactUnavailable
	}
	f, err := os.OpenFile(e.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	e.file, e.hash, e.created = f, sha256.New(), true
	return &writer{store: s, scope: scope, ctx: ctx}, nil
}

func (s *Local) Settle(ctx context.Context, scope contract.ArtifactCall, refs []contract.ArtifactRef) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.lookup(scope)
	if e == nil {
		return 0, contract.ErrArtifactUnavailable
	}
	key := callID{owner: scope.Owner, call: scope.CallID}
	if len(refs) == 1 && e.staged.ID != "" && !e.failed && sameRef(e.staged, refs[0]) && time.Now().Before(e.staged.ExpiresAt) {
		delete(s.calls, key)
		s.reserved -= scope.MaxBytes - e.used
		s.files[e.id] = e
		e.expiry = time.AfterFunc(time.Until(e.staged.ExpiresAt), func() { s.expire(e.id, e) })
		return e.used, nil
	}
	var err error
	if len(refs) > 0 {
		err = contract.ErrArtifactReceipt
	}
	if discardErr := discard(e); discardErr != nil {
		e.failed = true
		return e.used, errors.Join(err, contract.ErrArtifactCleanup, discardErr)
	}
	delete(s.calls, key)
	s.reserved -= scope.MaxBytes
	return e.used, err
}

func (s *Local) expire(id string, expected *item) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.files[id]
	if e != expected {
		return
	}
	if err := discard(e); err != nil {
		e.expiry = time.AfterFunc(time.Second, func() { s.expire(id, expected) })
		return
	}
	delete(s.files, id)
	s.reserved -= e.used
}

func sameRef(expected, given contract.ArtifactRef) bool {
	return expected.ID == given.ID && expected.Version == given.Version && expected.SizeBytes == given.SizeBytes &&
		expected.SHA256 == given.SHA256 && expected.ExpiresAt.Equal(given.ExpiresAt)
}

func discard(e *item) error {
	if e.file != nil {
		_ = e.file.Close()
		e.file = nil
	}
	if e.created {
		if err := os.Remove(e.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		e.created = false
	}
	return nil
}

func (s *Local) ReadArtifact(ctx context.Context, owner contract.ArtifactOwner, ref contract.ArtifactRef, offset, maxBytes uint64) (contract.ArtifactPage, error) {
	if err := ctx.Err(); err != nil {
		return contract.ArtifactPage{}, err
	}
	s.mu.Lock()
	e := s.files[ref.ID]
	if e == nil || e.scope.Owner != owner || !sameRef(e.staged, ref) || !time.Now().Before(e.staged.ExpiresAt) ||
		maxBytes == 0 || offset > e.staged.SizeBytes {
		s.mu.Unlock()
		return contract.ArtifactPage{}, contract.ErrArtifactUnavailable
	}
	f, err := os.Open(e.path)
	s.mu.Unlock()
	if err != nil {
		return contract.ArtifactPage{}, contract.ErrArtifactUnavailable
	}
	defer f.Close()
	next := offset + min(e.staged.SizeBytes-offset, maxBytes, s.cfg.MaxPageBytes)
	content := make([]byte, next-offset)
	if len(content) > 0 {
		if _, err := f.ReadAt(content, int64(offset)); err != nil {
			return contract.ArtifactPage{}, contract.ErrArtifactUnavailable
		}
	}
	return contract.ArtifactPage{Content: content, NextOffset: next, EOF: next == e.staged.SizeBytes}, nil
}

func (s *Local) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, e := range s.calls {
		_ = discard(e)
	}
	for _, e := range s.files {
		e.expiry.Stop()
	}
	clear(s.calls)
	clear(s.files)
	return os.RemoveAll(s.path)
}

type writer struct {
	store *Local
	scope contract.ArtifactCall
	ctx   context.Context
}

func (w *writer) Write(p []byte) (int, error) {
	s := w.store
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.lookup(w.scope)
	if e == nil || e.file == nil || e.failed {
		return 0, contract.ErrArtifactUnavailable
	}
	if err := w.ctx.Err(); err != nil {
		e.failed = true
		return 0, err
	}
	if len(p) > contract.ArtifactMaxBlockBytes {
		e.failed = true
		return 0, contract.ErrArtifactOutputLimit
	}
	count := min(uint64(len(p)), e.scope.MaxBytes-e.used)
	n, err := e.file.Write(p[:count])
	e.used += uint64(n)
	_, _ = e.hash.Write(p[:n])
	if err != nil || n < int(count) {
		e.failed = true
		return n, errors.Join(err, io.ErrShortWrite)
	}
	if count < uint64(len(p)) {
		e.failed = true
		return n, contract.ErrArtifactOutputLimit
	}
	return n, nil
}

func (w *writer) Complete() (contract.ArtifactRef, error) {
	s := w.store
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.lookup(w.scope)
	if e == nil || e.file == nil || e.failed || w.ctx.Err() != nil {
		return contract.ArtifactRef{}, contract.ErrArtifactUnavailable
	}
	info, statErr := e.file.Stat()
	closeErr := e.file.Close()
	e.file = nil
	if statErr != nil || closeErr != nil || info.Size() != int64(e.used) {
		e.failed = true
		return contract.ArtifactRef{}, contract.ErrArtifactUnavailable
	}
	e.staged = contract.ArtifactRef{
		ID: e.id, Version: rand.Text(), SizeBytes: e.used,
		SHA256: hex.EncodeToString(e.hash.Sum(nil)), ExpiresAt: time.Now().UTC().Add(s.cfg.Retention),
	}
	return e.staged, nil
}

func (w *writer) Close() error {
	s := w.store
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.lookup(w.scope)
	if e == nil || e.staged.ID != "" {
		return nil
	}
	e.failed = true
	return discard(e)
}
