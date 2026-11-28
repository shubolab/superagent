package artifact

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

type Retained struct {
	cfg    Config
	root   os.FileInfo
	id     string
	closed atomic.Bool
	mu     sync.Mutex
	active map[*retainedWriter]struct{}
}

type artifactCatalog struct {
	ID      string
	Format  int
	Policy  Config
	Entries map[string]retainedEntry
}

type retainedEntry struct {
	Call      contract.ArtifactCall
	Deadline  time.Time
	Epoch     uint64
	Version   string
	Key       contract.ReceiptKey  `json:",omitzero"`
	Ref       contract.ArtifactRef `json:",omitzero"`
	Published bool                 `json:",omitempty"`
}

func NewRetained(cfg Config) (*Retained, error) {
	if !durable.Supported || !validConfig(cfg) {
		return nil, contract.ErrInvalidConfiguration
	}
	if err := durable.Directory(cfg.Directory); err != nil {
		return nil, err
	}
	root, err := os.Lstat(cfg.Directory)
	if err != nil || !root.IsDir() || root.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%w: artifact directory must be private", contract.ErrInvalidConfiguration)
	}
	s := &Retained{cfg: cfg, root: root, active: make(map[*retainedWriter]struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	policy := cfg
	policy.Directory = ""
	err = s.locked(ctx, func() error {
		var catalog artifactCatalog
		err := durable.Read(s.catalogPath(), &catalog)
		fresh := errors.Is(err, os.ErrNotExist)
		if err != nil && !fresh {
			return err
		}
		if fresh {
			if err := s.cleanCatalogStaging(true); err != nil {
				return err
			}
			catalog = artifactCatalog{ID: newArtifactID(), Format: 1, Policy: policy, Entries: make(map[string]retainedEntry)}
			if _, err := durable.Write(s.catalogPath(), catalog); err != nil {
				return err
			}
		}
		if catalog.Format != 1 || catalog.Policy != policy || !contract.ValidContentHash(catalog.ID) ||
			!s.validEntries(catalog) {
			return contract.ErrInvalidConfiguration
		}
		if !fresh {
			if err := s.cleanCatalogStaging(false); err != nil {
				return err
			}
		}
		s.id = catalog.ID
		return s.reap(&catalog, time.Now())
	})
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (*Retained) RetainsAcrossRestart() bool   { return true }
func (*Retained) StagesUntilPublication() bool { return true }
func (s *Retained) ArtifactStoreID() string    { return s.id }
func (s *Retained) catalogPath() string        { return filepath.Join(s.cfg.Directory, "catalog.json") }
func newArtifactID() string                    { return contract.HashBytes([]byte(rand.Text())) }
func (s *Retained) body(e retainedEntry) string {
	return filepath.Join(s.cfg.Directory, e.Version+".body")
}
func (s *Retained) checks(e retainedEntry) string {
	return filepath.Join(s.cfg.Directory, e.Version+".blocks")
}
func (s *Retained) writerPin(e retainedEntry) string {
	return filepath.Join(s.cfg.Directory, e.Version+".writer")
}

// The directory lock excludes every catalog writer while abandoned atomic-write files are removed.
func (s *Retained) cleanCatalogStaging(fresh bool) error {
	dir, err := os.Open(s.cfg.Directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	for {
		entries, readErr := dir.ReadDir(32)
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".record-") && entry.Type().IsRegular() {
				if err := os.Remove(filepath.Join(s.cfg.Directory, entry.Name())); err != nil {
					return err
				}
			} else if fresh && entry.Name() != "catalog.json.lock" {
				return contract.ErrInvalidConfiguration
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func artifactCallID(call contract.ArtifactCall) string {
	id, _ := durable.Digest([4]string{string(call.Owner.Runner), string(call.Owner.Subject), string(call.Owner.RunID), string(call.CallID)})
	return id
}

func (s *Retained) locked(ctx context.Context, fn func() error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.closed.Load() {
			return contract.ErrArtifactUnavailable
		}
		root, err := os.Lstat(s.cfg.Directory)
		if err != nil || !os.SameFile(root, s.root) || root.Mode().Perm()&0077 != 0 {
			return contract.ErrArtifactUnavailable
		}
		lock, err := durable.Lock(s.catalogPath())
		if err == nil {
			defer lock.Close()
			root, err := os.Lstat(s.cfg.Directory)
			if err != nil || !os.SameFile(root, s.root) || root.Mode().Perm()&0077 != 0 {
				return contract.ErrArtifactUnavailable
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return fn()
		}
		if !errors.Is(err, durable.ErrLocked) {
			return err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *Retained) withCatalog(ctx context.Context, fn func(*artifactCatalog) error) error {
	return s.locked(ctx, func() error {
		var catalog artifactCatalog
		policy := s.cfg
		policy.Directory = ""
		if err := durable.Read(s.catalogPath(), &catalog); err != nil || catalog.ID != s.id ||
			catalog.Format != 1 || catalog.Policy != policy || !s.validEntries(catalog) {
			return contract.ErrArtifactUnavailable
		}
		return fn(&catalog)
	})
}

func (s *Retained) validEntries(c artifactCatalog) bool {
	if c.Entries == nil || len(c.Entries) > s.cfg.MaxArtifacts {
		return false
	}
	var reserved uint64
	for id, e := range c.Entries {
		if !contract.ValidContentHash(e.Version) || e.Call.MaxBytes == 0 || e.Call.MaxBytes > s.cfg.MaxArtifactBytes ||
			e.Deadline.IsZero() || id != artifactCallID(e.Call) ||
			(e.Ref.ID == "" && (e.Ref != (contract.ArtifactRef{}) || e.Published)) ||
			(e.Ref.ID != "" && (e.Ref.ID != c.ID+id || e.Ref.Version != e.Version ||
				e.Ref.SizeBytes > e.Call.MaxBytes || e.Ref.ExpiresAt.IsZero() || !contract.ValidContentHash(e.Ref.SHA256))) ||
			e.bytes() > s.cfg.MaxStoredBytes-reserved {
			return false
		}
		reserved += e.bytes()
	}
	return true
}

func (e retainedEntry) bytes() uint64 {
	if e.Ref.ID != "" {
		return e.Ref.SizeBytes
	}
	return e.Call.MaxBytes
}

func (s *Retained) remove(e retainedEntry) error {
	pin, err := durable.Lock(s.writerPin(e))
	if err != nil {
		return err
	}
	defer pin.Close()
	for _, path := range []string{s.body(e), s.checks(e), s.writerPin(e) + ".lock"} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *Retained) reap(c *artifactCatalog, now time.Time) error {
	changed := false
	for id, e := range c.Entries {
		until := e.Deadline
		if e.Ref.ID != "" {
			until = e.Ref.ExpiresAt
		}
		if !now.Before(until) && s.remove(e) == nil {
			delete(c.Entries, id)
			changed = true
		}
	}
	if changed {
		_, err := durable.Write(s.catalogPath(), *c)
		return err
	}
	return nil
}

func (s *Retained) Begin(ctx context.Context, call contract.ArtifactCall) (uint64, error) {
	admission, err := contract.VerifyArtifactAccess(ctx, contract.ArtifactReserve, call, contract.ArtifactRef{})
	if err != nil || call.Owner.Runner == "" || call.Owner.Subject == "" || call.Owner.RunID == "" ||
		call.CallID == "" || call.Tool == "" || call.MaxBytes == 0 || !time.Now().Before(admission.Deadline) {
		return 0, contract.ErrAccessDenied
	}
	var limit uint64
	err = s.withCatalog(ctx, func(c *artifactCatalog) error {
		if err := s.reap(c, time.Now()); err != nil {
			return err
		}
		id := artifactCallID(call)
		if _, exists := c.Entries[id]; exists {
			return contract.ErrArtifactCapacity
		}
		var used uint64
		for _, e := range c.Entries {
			used += e.bytes()
		}
		limit = min(call.MaxBytes, s.cfg.MaxArtifactBytes, s.cfg.MaxStoredBytes-used)
		if limit == 0 || len(c.Entries) >= s.cfg.MaxArtifacts {
			return contract.ErrArtifactCapacity
		}
		call.MaxBytes = limit
		c.Entries[id] = retainedEntry{Call: call, Deadline: admission.Deadline, Epoch: admission.Epoch, Version: newArtifactID()}
		_, err := durable.Write(s.catalogPath(), *c)
		return err
	})
	if err != nil {
		return 0, err
	}
	return limit, nil
}

func (s *Retained) Settle(ctx context.Context, call contract.ArtifactCall, refs []contract.ArtifactRef) (uint64, error) {
	if len(refs) == 1 {
		publish, err := s.prepareArtifactPublication(ctx, contract.ArtifactPublish, call, refs[0])
		if err != nil {
			return 0, err
		}
		return publish(ctx)
	}
	admission, err := contract.VerifyArtifactAccess(ctx, contract.ArtifactDiscard, call, contract.ArtifactRef{})
	if err != nil || len(refs) != 0 {
		return 0, contract.ErrArtifactReceipt
	}
	var used uint64
	err = s.withCatalog(ctx, func(c *artifactCatalog) error {
		id := artifactCallID(call)
		e, exists := c.Entries[id]
		if !exists {
			return nil
		}
		if e.Call != call || e.Published || e.Epoch != admission.Epoch || !e.Deadline.Equal(admission.Deadline) {
			return contract.ErrArtifactReceipt
		}
		if e.Ref.ID != "" {
			used = e.Ref.SizeBytes
		} else if info, err := os.Stat(s.body(e)); err == nil && info.Size() >= 0 && uint64(info.Size()) <= call.MaxBytes {
			used = uint64(info.Size())
		}
		if err := s.remove(e); err != nil {
			return errors.Join(contract.ErrArtifactCleanup, err)
		}
		delete(c.Entries, id)
		_, err := durable.Write(s.catalogPath(), *c)
		return err
	})
	return used, err
}

func (s *Retained) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.mu.Lock()
	writers := make([]*retainedWriter, 0, len(s.active))
	for w := range s.active {
		writers = append(writers, w)
	}
	s.mu.Unlock()
	var err error
	for _, w := range writers {
		err = errors.Join(err, w.Close())
	}
	return err
}
