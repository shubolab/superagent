package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

type retainedWriter struct {
	store             *Retained
	ctx               context.Context
	admission         contract.ArtifactAdmission
	entry             retainedEntry
	body, checks, pin *os.File
	sum, blockSum     hash.Hash
	mu                sync.Mutex
	used              uint64
	blockBytes        int
	failed            bool
}

func (s *Retained) OpenWriter(ctx context.Context, call contract.ArtifactCall) (contract.ArtifactWriter, error) {
	admission, ok := contract.ArtifactAdmissionFrom(ctx)
	if !ok || (admission.Epoch != 0 && admission.Seal == nil) || !matchesArtifactKey(call, admission.Key, admission.Epoch) {
		return nil, contract.ErrAccessDenied
	}
	var w *retainedWriter
	err := s.withCatalog(ctx, func(c *artifactCatalog) error {
		e, ok := c.Entries[artifactCallID(call)]
		if !ok || e.Call != call || e.Ref.ID != "" || e.Epoch != admission.Epoch || !e.Deadline.Equal(admission.Deadline) ||
			!time.Now().Before(e.Deadline) {
			return contract.ErrArtifactUnavailable
		}
		pin, err := durable.Lock(s.writerPin(e))
		if err != nil {
			return err
		}
		body, err := os.OpenFile(s.body(e), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			_ = pin.Close()
			return err
		}
		checks, err := os.OpenFile(s.checks(e), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			_ = body.Close()
			_ = pin.Close()
			return err
		}
		w = &retainedWriter{store: s, ctx: ctx, admission: admission, entry: e, body: body, checks: checks, pin: pin,
			sum: sha256.New(), blockSum: sha256.New()}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.closed.Load() {
			_ = w.closeFiles()
			_ = pin.Close()
			return contract.ErrArtifactUnavailable
		}
		s.active[w] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

func matchesArtifactKey(call contract.ArtifactCall, key contract.ReceiptKey, epoch uint64) bool {
	if key == (contract.ReceiptKey{}) {
		return epoch == 0
	}
	return key.Runner == call.Owner.Runner && key.Subject == call.Owner.Subject && key.RunID == call.Owner.RunID &&
		key.CallID == call.CallID && key.Tool == call.Tool && key.Step != 0 && key.AdmissionEpoch == epoch &&
		contract.ValidContentHash(key.Fingerprint)
}

func (w *retainedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pin == nil || w.failed {
		return 0, contract.ErrArtifactUnavailable
	}
	if err := w.ctx.Err(); err != nil {
		w.failed = true
		return 0, err
	}
	if w.store.closed.Load() || !time.Now().Before(w.entry.Deadline) {
		w.failed = true
		return 0, contract.ErrArtifactUnavailable
	}
	if len(p) > contract.ArtifactMaxBlockBytes {
		w.failed = true
		return 0, contract.ErrArtifactOutputLimit
	}
	limit := min(uint64(len(p)), w.entry.Call.MaxBytes-w.used)
	n, err := w.body.Write(p[:limit])
	w.used += uint64(n)
	if err == nil && uint64(n) != limit {
		err = io.ErrShortWrite
	}
	if err == nil && limit < uint64(len(p)) {
		err = contract.ErrArtifactOutputLimit
	}
	if err == nil {
		err = w.hashBlockBytes(p[:n])
	}
	if err != nil {
		w.failed = true
	}
	return n, err
}

func (w *retainedWriter) hashBlockBytes(p []byte) error {
	_, _ = w.sum.Write(p)
	for len(p) > 0 {
		n := min(len(p), contract.ArtifactMaxBlockBytes-w.blockBytes)
		_, _ = w.blockSum.Write(p[:n])
		w.blockBytes += n
		p = p[n:]
		if w.blockBytes == contract.ArtifactMaxBlockBytes {
			if err := w.flushBlock(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *retainedWriter) flushBlock() error {
	_, err := w.checks.Write(w.blockSum.Sum(nil))
	w.blockSum.Reset()
	w.blockBytes = 0
	return err
}

func (w *retainedWriter) Complete() (contract.ArtifactRef, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pin == nil || w.failed || w.ctx.Err() != nil || !time.Now().Before(w.entry.Deadline) {
		return contract.ArtifactRef{}, contract.ErrArtifactUnavailable
	}
	defer w.release()
	if w.blockBytes != 0 {
		if err := w.flushBlock(); err != nil {
			return contract.ArtifactRef{}, err
		}
	}
	if err := errors.Join(w.body.Sync(), w.checks.Sync(), w.closeFiles()); err != nil {
		return contract.ArtifactRef{}, err
	}
	s := w.store
	id := artifactCallID(w.entry.Call)
	expires := time.Now().UTC().Add(s.cfg.Retention)
	if w.admission.Key != (contract.ReceiptKey{}) && expires.Before(w.entry.Deadline) {
		expires = w.entry.Deadline.UTC()
	}
	ref := contract.ArtifactRef{ID: s.id + id, Version: w.entry.Version,
		SizeBytes: w.used, SHA256: hex.EncodeToString(w.sum.Sum(nil)), ExpiresAt: expires}
	seal := func() error {
		return s.withCatalog(w.ctx, func(c *artifactCatalog) error {
			e := c.Entries[id]
			if e.Version != w.entry.Version || e.Ref.ID != "" || !time.Now().Before(w.entry.Deadline) {
				return contract.ErrArtifactUnavailable
			}
			e.Ref, e.Key = ref, w.admission.Key
			c.Entries[id] = e
			_, err := durable.Write(s.catalogPath(), *c)
			return err
		})
	}
	var err error
	if w.admission.Seal != nil {
		err = w.admission.Seal(seal)
	} else {
		err = seal()
	}
	if err != nil {
		return contract.ArtifactRef{}, err
	}
	return ref, nil
}

func (w *retainedWriter) closeFiles() error {
	if w.body == nil {
		return nil
	}
	err := errors.Join(w.body.Close(), w.checks.Close())
	w.body, w.checks = nil, nil
	return err
}

func (w *retainedWriter) release() error {
	if w.pin == nil {
		return nil
	}
	err := errors.Join(w.closeFiles(), w.pin.Close())
	w.pin = nil
	w.store.mu.Lock()
	delete(w.store.active, w)
	w.store.mu.Unlock()
	return err
}

func (w *retainedWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.release()
}
