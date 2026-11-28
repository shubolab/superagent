package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

type sealedFiles struct {
	entry                retainedEntry
	body, checks         *os.File
	bodyInfo, checksInfo os.FileInfo
}

func (f sealedFiles) close() {
	_ = f.body.Close()
	_ = f.checks.Close()
}

func blocksFor(size uint64) uint64 {
	return (size + contract.ArtifactMaxBlockBytes - 1) / contract.ArtifactMaxBlockBytes
}

func artifactFile(path string, size uint64) (*os.File, os.FileInfo, error) {
	listed, err := os.Lstat(path)
	if err != nil || !listed.Mode().IsRegular() || uint64(listed.Size()) != size {
		return nil, nil, contract.ErrArtifactUnavailable
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, contract.ErrArtifactUnavailable
	}
	actual, err := f.Stat()
	if err != nil || !os.SameFile(listed, actual) || uint64(actual.Size()) != size {
		_ = f.Close()
		return nil, nil, contract.ErrArtifactUnavailable
	}
	return f, actual, nil
}

func (s *Retained) openSealed(ctx context.Context, owner contract.ArtifactOwner, ref contract.ArtifactRef, published bool) (sealedFiles, error) {
	var opened sealedFiles
	id, ok := strings.CutPrefix(ref.ID, s.id)
	if !ok || !contract.ValidContentHash(id) {
		return opened, contract.ErrArtifactUnavailable
	}
	err := s.withCatalog(ctx, func(c *artifactCatalog) error {
		e := c.Entries[id]
		if e.Ref.ID == "" || e.Call.Owner != owner || !sameRef(e.Ref, ref) ||
			(published && !e.Published) || !time.Now().Before(e.Ref.ExpiresAt) {
			return contract.ErrArtifactUnavailable
		}
		if err := durable.SyncDirectory(s.cfg.Directory); err != nil {
			return err
		}
		body, bodyInfo, err := artifactFile(s.body(e), ref.SizeBytes)
		if err != nil {
			return err
		}
		checks, checksInfo, err := artifactFile(s.checks(e), blocksFor(ref.SizeBytes)*sha256.Size)
		if err != nil {
			_ = body.Close()
			return err
		}
		opened = sealedFiles{entry: e, body: body, checks: checks, bodyInfo: bodyInfo, checksInfo: checksInfo}
		return nil
	})
	if err != nil && ctx.Err() == nil {
		err = contract.ErrArtifactUnavailable
	}
	return opened, err
}

func (f sealedFiles) readBlock(dst []byte, offset uint64) error {
	var expected [sha256.Size]byte
	if _, err := f.body.ReadAt(dst, int64(offset)); err != nil {
		return contract.ErrArtifactUnavailable
	}
	index := offset / contract.ArtifactMaxBlockBytes * sha256.Size
	if _, err := f.checks.ReadAt(expected[:], int64(index)); err != nil || sha256.Sum256(dst) != expected {
		return contract.ErrArtifactUnavailable
	}
	return nil
}

func (f sealedFiles) verify(ctx context.Context) error {
	size := f.entry.Ref.SizeBytes
	hash := sha256.New()
	buf := make([]byte, min(size, contract.ArtifactMaxBlockBytes))
	for offset := uint64(0); offset < size; offset += contract.ArtifactMaxBlockBytes {
		if err := ctx.Err(); err != nil {
			return err
		}
		part := buf[:min(uint64(len(buf)), size-offset)]
		if err := f.readBlock(part, offset); err != nil {
			return err
		}
		_, _ = hash.Write(part)
	}
	if hex.EncodeToString(hash.Sum(nil)) != f.entry.Ref.SHA256 || !time.Now().Before(f.entry.Ref.ExpiresAt) {
		return contract.ErrArtifactUnavailable
	}
	return ctx.Err()
}

func (s *Retained) CheckArtifact(ctx context.Context, owner contract.ArtifactOwner, ref contract.ArtifactRef) error {
	if _, err := contract.VerifyArtifactAccess(ctx, contract.ArtifactCheck, contract.ArtifactCall{Owner: owner}, ref); err != nil {
		return err
	}
	f, err := s.openSealed(ctx, owner, ref, true)
	if err != nil {
		return err
	}
	defer f.close()
	return f.verify(ctx)
}

func (s *Retained) PrepareArtifactPublication(ctx context.Context, call contract.ArtifactCall, ref contract.ArtifactRef) (func(context.Context) (uint64, error), error) {
	return s.prepareArtifactPublication(ctx, contract.ArtifactPreparePublication, call, ref)
}

func (s *Retained) prepareArtifactPublication(ctx context.Context, op contract.ArtifactOperation, call contract.ArtifactCall, ref contract.ArtifactRef) (func(context.Context) (uint64, error), error) {
	admission, err := contract.VerifyArtifactAccess(ctx, op, call, ref)
	key := admission.ReceiptKey
	if err != nil || ref.ID != s.id+artifactCallID(call) || !matchesArtifactKey(call, key, key.AdmissionEpoch) {
		return nil, contract.ErrAccessDenied
	}
	f, err := s.openSealed(ctx, call.Owner, ref, false)
	if err != nil {
		return nil, err
	}
	defer f.close()
	e := f.entry
	if e.Call != call || e.Key != key || e.Epoch != key.AdmissionEpoch || e.Epoch > admission.Epoch ||
		!e.Deadline.Equal(admission.Deadline) || !time.Now().Before(e.Deadline) {
		return nil, contract.ErrArtifactReceipt
	}
	if err := f.verify(ctx); err != nil {
		return nil, err
	}
	return func(commitCtx context.Context) (uint64, error) {
		err := s.withCatalog(commitCtx, func(c *artifactCatalog) error {
			access, err := contract.VerifyArtifactAccess(commitCtx, contract.ArtifactPublish, call, ref)
			if err != nil || access != admission {
				return contract.ErrAccessDenied
			}
			id := artifactCallID(call)
			current := c.Entries[id]
			if !sameRef(current.Ref, ref) || current.Key != key ||
				!time.Now().Before(minDeadline(current.Deadline, ref.ExpiresAt)) ||
				!unchangedArtifactFile(s.body(current), f.bodyInfo) || !unchangedArtifactFile(s.checks(current), f.checksInfo) {
				return contract.ErrArtifactUnavailable
			}
			if current.Published {
				return nil
			}
			if err := commitCtx.Err(); err != nil {
				return err
			}
			current.Published = true
			c.Entries[id] = current
			_, err = durable.Write(s.catalogPath(), *c)
			return err
		})
		if err != nil {
			return 0, err
		}
		return ref.SizeBytes, nil
	}, nil
}

func minDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func unchangedArtifactFile(path string, previous os.FileInfo) bool {
	info, err := os.Lstat(path)
	return err == nil && os.SameFile(info, previous) && info.Size() == previous.Size() && info.ModTime().Equal(previous.ModTime())
}

func (s *Retained) ReadArtifact(ctx context.Context, owner contract.ArtifactOwner, ref contract.ArtifactRef, offset, maxBytes uint64) (contract.ArtifactPage, error) {
	if _, err := contract.VerifyArtifactAccess(ctx, contract.ArtifactRead, contract.ArtifactCall{Owner: owner}, ref); err != nil {
		return contract.ArtifactPage{}, err
	}
	if offset > ref.SizeBytes || maxBytes == 0 {
		return contract.ArtifactPage{}, contract.ErrArtifactUnavailable
	}
	f, err := s.openSealed(ctx, owner, ref, true)
	if err != nil {
		return contract.ArtifactPage{}, err
	}
	defer f.close()
	next := offset + min(maxBytes, s.cfg.MaxPageBytes, ref.SizeBytes-offset)
	result := make([]byte, 0, next-offset)
	if offset < next {
		const size = contract.ArtifactMaxBlockBytes
		buf := make([]byte, min(ref.SizeBytes, size))
		for start := offset / size * size; start < next; start += size {
			chunk := buf[:min(uint64(size), ref.SizeBytes-start)]
			if err := f.readBlock(chunk, start); err != nil {
				return contract.ArtifactPage{}, err
			}
			result = append(result, chunk[max(start, offset)-start:min(next-start, uint64(len(chunk)))]...)
		}
	}
	if err := ctx.Err(); err != nil {
		return contract.ArtifactPage{}, err
	}
	if !time.Now().Before(ref.ExpiresAt) {
		return contract.ArtifactPage{}, contract.ErrArtifactUnavailable
	}
	return contract.ArtifactPage{Content: result, NextOffset: next, EOF: next == ref.SizeBytes}, nil
}
