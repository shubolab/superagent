package environment

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

type EvidenceWorkspaceConfig struct {
	Directory    string
	MaxFileBytes uint64
}

type EvidenceLocal struct {
	*TakeoverLocal
	dir      string
	maxBytes uint64
	now      func() time.Time
}

type workspaceIndex struct {
	Paths  map[string]contract.FileVersion
	Writes map[string]contract.WriteEvidence
}

func NewEvidenceLocal(workspaces map[contract.WorkspaceRef]WorkspaceConfig, cfg EvidenceWorkspaceConfig) (*EvidenceLocal, error) {
	if !durable.Supported || cfg.MaxFileBytes == 0 || cfg.MaxFileBytes > durable.MaxRecordBytes {
		return nil, contract.ErrInvalidConfiguration
	}
	local, err := NewTakeoverLocal(workspaces)
	if err != nil {
		return nil, err
	}
	if err := durable.Directory(cfg.Directory); err != nil {
		return nil, err
	}
	for ref := range workspaces {
		version, err := local.RecoveryWorkspace(context.Background(), contract.RunScope{Workspace: ref})
		if err != nil {
			return nil, err
		}
		dir := workspaceDirectory(cfg.Directory, ref, version)
		if err := durable.Directory(dir); err != nil {
			return nil, err
		}
		lock, err := durable.Lock(filepath.Join(dir, "index.json"))
		if err != nil && !errors.Is(err, durable.ErrLocked) {
			return nil, err
		}
		if lock != nil {
			_ = lock.Close()
		}
	}
	return &EvidenceLocal{TakeoverLocal: local, dir: cfg.Directory, maxBytes: cfg.MaxFileBytes, now: time.Now}, nil
}

func workspaceDirectory(root string, ref contract.WorkspaceRef, version string) string {
	id, _ := durable.Digest([2]string{string(ref), version})
	return filepath.Join(root, id)
}

func (l *EvidenceLocal) indexPath(ctx context.Context, scope contract.RunScope, expected string) (string, error) {
	version, err := l.RecoveryWorkspace(ctx, scope)
	if err != nil || (expected != "" && version != expected) {
		return "", contract.ErrAccessDenied
	}
	return filepath.Join(workspaceDirectory(l.dir, scope.Workspace, version), "index.json"), nil
}

func loadWorkspaceIndex(path string) (workspaceIndex, error) {
	var index workspaceIndex
	err := durable.Read(path, &index)
	if errors.Is(err, os.ErrNotExist) {
		return workspaceIndex{Paths: make(map[string]contract.FileVersion), Writes: make(map[string]contract.WriteEvidence)}, nil
	}
	if err != nil {
		return index, err
	}
	if index.Paths == nil || index.Writes == nil {
		return index, contract.ErrWriteEvidenceUnavailable
	}
	return index, nil
}

// PublishWorkspace denies direct appends. Managed writes use CommitWrite.
func (*EvidenceLocal) PublishWorkspace(context.Context, contract.CallScope, contract.EnvironmentGrantRef, []byte) error {
	return contract.ErrAccessDenied
}

func (l *EvidenceLocal) writeIndex(ctx context.Context, scope contract.CallScope, grant contract.EnvironmentGrantRef, intent contract.WriteIntent) (string, string, error) {
	key := intent.Key
	if key.Remote.EvidenceRule != contract.AppendFileEvidenceV1 || key.Remote.WorkspaceVersion == "" ||
		key.Remote.Workspace != scope.Run.Workspace || key.Subject != scope.Run.Subject || key.Runner != scope.Run.Runner ||
		key.RunID != scope.Run.RunID || key.AdmissionEpoch != scope.Run.Epoch || key.CallID != scope.CallID ||
		key.Tool != scope.Tool || key.Step == 0 || !contract.ValidContentHash(key.Fingerprint) ||
		intent.Path != scope.Path || intent.Limits != scope.Limits || !contract.ValidManagedPath(intent.Path) ||
		!contract.ValidContentHash(intent.AppendHash) || intent.AppendBytes == 0 || intent.AppendBytes > contract.MaxWorkspacePublishBytes || intent.RetainUntil.Before(scope.Run.Deadline) ||
		expired(ctx, scope.Run) {
		return "", "", contract.ErrAccessDenied
	}
	_, g := l.findGrant(scope, grant)
	if g == nil {
		return "", "", contract.ErrAccessDenied
	}
	path, err := l.indexPath(ctx, scope.Run, key.Remote.WorkspaceVersion)
	return path, durable.WriteEvidenceID(key), err
}

func (l *EvidenceLocal) readVersion(index string, version contract.FileVersion) ([]byte, error) {
	if !contract.ValidContentHash(version.ID) || version.Bytes == 0 || version.Bytes > l.maxBytes {
		return nil, contract.ErrWriteEvidenceUnavailable
	}
	f, err := os.Open(blobPath(index, version.ID))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || uint64(info.Size()) != version.Bytes {
		return nil, contract.ErrWriteEvidenceUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(version.Bytes)+1))
	if err != nil || uint64(len(data)) != version.Bytes || contract.HashBytes(data) != version.ID {
		return nil, contract.ErrWriteEvidenceUnavailable
	}
	return data, nil
}

func blobPath(index, version string) string {
	return filepath.Join(filepath.Dir(index), version+".blob")
}

func (l *EvidenceLocal) StageWrite(ctx context.Context, scope contract.CallScope, grant contract.EnvironmentGrantRef, intent contract.WriteIntent, data []byte) (contract.StagedWrite, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var staged contract.StagedWrite
	if err := contract.VerifyWorkspaceWrite(ctx, scope, grant, intent, nil); err != nil {
		return staged, err
	}
	index, id, err := l.writeIndex(ctx, scope, grant, intent)
	if err != nil {
		return staged, err
	}
	if uint64(len(data)) != intent.AppendBytes || contract.HashBytes(data) != intent.AppendHash {
		return staged, contract.ErrInvalidRequest
	}
	lock, err := durable.Lock(index)
	if err != nil {
		return staged, err
	}
	defer lock.Close()
	state, err := loadWorkspaceIndex(index)
	if err != nil {
		return staged, err
	}
	if old, ok := state.Writes[id]; ok {
		if !old.Intent.Matches(intent) {
			return staged, contract.ErrStartConflict
		}
		return contract.StagedWrite{BaseVersion: old.BaseVersion, Version: old.Version}, nil
	}
	base := state.Paths[intent.Path]
	if base.Bytes > l.maxBytes || uint64(len(data)) > l.maxBytes-base.Bytes {
		return staged, contract.ErrArtifactOutputLimit
	}
	var contents []byte
	if base.ID != "" {
		contents, err = l.readVersion(index, base)
		if err != nil {
			return staged, err
		}
	}
	contents = append(contents, data...)
	version := contract.FileVersion{ID: contract.HashBytes(contents), Bytes: uint64(len(contents))}
	if _, err := os.Stat(blobPath(index, version.ID)); errors.Is(err, os.ErrNotExist) {
		if _, err := durable.WriteBytes(blobPath(index, version.ID), contents); err != nil {
			return staged, err
		}
	} else if _, err := l.readVersion(index, version); err != nil {
		return staged, err
	}
	return contract.StagedWrite{BaseVersion: base.ID, Version: version}, nil
}

func (l *EvidenceLocal) CommitWrite(ctx context.Context, scope contract.CallScope, grant contract.EnvironmentGrantRef, intent contract.WriteIntent, staged contract.StagedWrite) (contract.WriteEvidence, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var evidence contract.WriteEvidence
	if err := contract.VerifyWorkspaceWrite(ctx, scope, grant, intent, &staged); err != nil {
		return evidence, err
	}
	index, id, err := l.writeIndex(ctx, scope, grant, intent)
	if err != nil {
		return evidence, err
	}
	lock, err := durable.Lock(index)
	if err != nil {
		return evidence, err
	}
	defer lock.Close()
	state, err := loadWorkspaceIndex(index)
	if err != nil {
		return evidence, err
	}
	if old, ok := state.Writes[id]; ok {
		if !old.Intent.Matches(intent) || old.Version != staged.Version || old.BaseVersion != staged.BaseVersion {
			return evidence, contract.ErrStartConflict
		}
		return old, nil
	}
	base := state.Paths[intent.Path]
	if base.ID != staged.BaseVersion || base.Bytes > staged.Version.Bytes || staged.Version.Bytes-base.Bytes != intent.AppendBytes {
		return evidence, contract.ErrStartConflict
	}
	contents, err := l.readVersion(index, staged.Version)
	if err != nil || !matchesAppend(contents, base.ID, intent) {
		return evidence, contract.ErrWriteEvidenceUnavailable
	}
	evidence = contract.WriteEvidence{Intent: intent, BaseVersion: base.ID, Version: staged.Version,
		Ref: contract.WriteEvidenceRef{ID: id, Version: staged.Version.ID}}
	state.Paths[intent.Path], state.Writes[id] = staged.Version, evidence
	if expired(ctx, scope.Run) {
		return contract.WriteEvidence{}, contract.ErrAccessDenied
	}
	if err := contract.VerifyWorkspaceWrite(ctx, scope, grant, intent, &staged); err != nil {
		return contract.WriteEvidence{}, err
	}
	if _, err := durable.Write(index, state); err != nil {
		return contract.WriteEvidence{}, err
	}
	return evidence, nil
}

func (l *EvidenceLocal) ReadWriteEvidence(ctx context.Context, intent contract.WriteIntent) (contract.WriteEvidence, error) {
	var zero contract.WriteEvidence
	key := intent.Key
	if err := contract.VerifyWriteEvidenceRead(ctx, intent); err != nil {
		return zero, err
	}
	if !l.now().Before(intent.RetainUntil) {
		return zero, contract.ErrWriteEvidenceUnavailable
	}
	index, err := l.indexPath(ctx, contract.RunScope{Workspace: key.Remote.Workspace}, key.Remote.WorkspaceVersion)
	if err != nil {
		return zero, err
	}
	lock, err := durable.ReadLock(index)
	if err != nil {
		return zero, err
	}
	defer lock.Close()
	if err := durable.SyncDirectory(filepath.Dir(index)); err != nil {
		return zero, err
	}
	state, err := loadWorkspaceIndex(index)
	if err != nil {
		return zero, err
	}
	id := durable.WriteEvidenceID(key)
	evidence, ok := state.Writes[id]
	if !ok || !evidence.Intent.Matches(intent) || evidence.Ref.ID != id || evidence.Ref.Version != evidence.Version.ID {
		return zero, contract.ErrWriteEvidenceUnavailable
	}
	contents, err := l.readVersion(index, evidence.Version)
	if err != nil {
		return zero, fmt.Errorf("%w: %v", contract.ErrWriteEvidenceUnavailable, err)
	}
	if !matchesAppend(contents, evidence.BaseVersion, intent) {
		return zero, contract.ErrWriteEvidenceUnavailable
	}
	if err := contract.VerifyWriteEvidenceRead(ctx, intent); err != nil {
		return zero, err
	}
	return evidence, nil
}

func matchesAppend(data []byte, base string, intent contract.WriteIntent) bool {
	size := uint64(len(data))
	if intent.AppendBytes == 0 || intent.AppendBytes > size || intent.AppendBytes > contract.MaxWorkspacePublishBytes {
		return false
	}
	offset := size - intent.AppendBytes
	if contract.HashBytes(data[offset:]) != intent.AppendHash {
		return false
	}
	if base == "" {
		return offset == 0
	}
	return offset > 0 && contract.HashBytes(data[:offset]) == base
}

func (l *EvidenceLocal) OpenRead(ctx context.Context, scope contract.CallScope, ref contract.EnvironmentGrantRef, path string) (io.ReadCloser, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, g := l.findGrant(scope, ref)
	if g == nil || g.reader != nil || scope.Path != path || expired(ctx, scope.Run) {
		return nil, contract.ErrAccessDenied
	}
	index, err := l.indexPath(ctx, scope.Run, "")
	if err != nil {
		return nil, err
	}
	lock, err := durable.ReadLock(index)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if err := durable.SyncDirectory(filepath.Dir(index)); err != nil {
		return nil, err
	}
	state, err := loadWorkspaceIndex(index)
	if err != nil {
		return nil, err
	}
	version, ok := state.Paths[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	if !contract.ValidContentHash(version.ID) {
		return nil, contract.ErrWriteEvidenceUnavailable
	}
	f, err := os.Open(blobPath(index, version.ID))
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || uint64(info.Size()) != version.Bytes || version.Bytes > l.maxBytes {
		_ = f.Close()
		return nil, contract.ErrWriteEvidenceUnavailable
	}
	return startRead(ctx, scope, g, f), nil
}
