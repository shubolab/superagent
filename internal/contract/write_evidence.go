package contract

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"strings"
	"time"
)

const AppendFileEvidenceV1 = "append-file/v1"

var ErrWriteEvidenceUnavailable = errors.New("write evidence unavailable")

type WriteIntent struct {
	Key         ReceiptKey
	Path        string
	AppendHash  string
	AppendBytes uint64
	Limits      ToolLimits
	RetainUntil time.Time
}

func (i WriteIntent) Matches(other WriteIntent) bool {
	return i.Key == other.Key && i.Path == other.Path && i.AppendHash == other.AppendHash &&
		i.AppendBytes == other.AppendBytes && i.Limits == other.Limits && i.RetainUntil.Equal(other.RetainUntil)
}

type AppendFileResult struct {
	Path    string
	Version string
	Bytes   uint64
	SHA256  string
}

type WriteEvidenceRef struct {
	ID      string
	Version string
}

type FileVersion struct {
	ID    string
	Bytes uint64
}

type WriteEvidence struct {
	Intent      WriteIntent
	BaseVersion string
	Version     FileVersion
	Ref         WriteEvidenceRef
}

// StagedWrite identifies an immutable version that is not yet visible at the path.
type StagedWrite struct {
	BaseVersion string
	Version     FileVersion
}

// EvidenceWorkspace stages a private immutable version, then commits the visible
// path and its original-call proof together. A retry of a committed intent returns
// the existing proof. ReadWriteEvidence verifies the immutable original version.
type EvidenceWorkspace interface {
	TakeoverWorkspace
	StageWrite(context.Context, CallScope, EnvironmentGrantRef, WriteIntent, []byte) (StagedWrite, error)
	CommitWrite(context.Context, CallScope, EnvironmentGrantRef, WriteIntent, StagedWrite) (WriteEvidence, error)
	ReadWriteEvidence(context.Context, WriteIntent) (WriteEvidence, error)
}

func WriteEvidenceID(key ReceiptKey) string {
	data, _ := json.Marshal([4]string{string(key.Runner), string(key.Subject), string(key.RunID), string(key.CallID)})
	return HashBytes(data)
}

type workspaceWriteAccess struct {
	scope  CallScope
	grant  EnvironmentGrantRef
	intent WriteIntent
	staged *StagedWrite
	permit *storagePermit
}

type workspaceWriteKey struct{}

func WithWorkspaceWriteAccess(ctx context.Context, scope CallScope, grant EnvironmentGrantRef, intent WriteIntent, staged *StagedWrite) (context.Context, func()) {
	permit := &storagePermit{ctx: ctx}
	access := workspaceWriteAccess{scope: scope, grant: grant, intent: intent, staged: staged, permit: permit}
	return context.WithValue(ctx, workspaceWriteKey{}, access), permit.revoke
}

func VerifyWorkspaceWrite(ctx context.Context, scope CallScope, grant EnvironmentGrantRef, intent WriteIntent, staged *StagedWrite) error {
	access, ok := ctx.Value(workspaceWriteKey{}).(workspaceWriteAccess)
	if !ok || !access.permit.active() || access.scope != scope || grant == "" || access.grant != grant ||
		!access.intent.Matches(intent) || (access.staged == nil) != (staged == nil) ||
		(staged != nil && *access.staged != *staged) || !time.Now().Before(scope.Run.Deadline) {
		return ErrAccessDenied
	}
	return ctx.Err()
}

func ValidManagedPath(path string) bool {
	return path != "." && len(path) <= 512 && fs.ValidPath(path) && !strings.Contains(path, "\\")
}

func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ValidContentHash(hash string) bool {
	if len(hash) != 2*sha256.Size {
		return false
	}
	for _, c := range hash {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ParseEvidenceAppend is the complete operation for the append-file/v1 binding.
func ParseEvidenceAppend(raw json.RawMessage) (path string, data []byte, err error) {
	var args struct {
		Path    string
		Content string
	}
	if !JSONObject(raw) || json.Unmarshal(raw, &args) != nil || !ValidManagedPath(args.Path) ||
		len(args.Content) == 0 || len(args.Content) > MaxWorkspacePublishBytes {
		return "", nil, ErrInvalidRequest
	}
	return args.Path, []byte(args.Content), nil
}

type writeEvidenceQueryKey struct{}

type writeEvidenceQuery struct {
	intent WriteIntent
	permit *storagePermit
}

func WithWriteEvidenceQuery(ctx context.Context, intent WriteIntent) (context.Context, func()) {
	permit := &storagePermit{ctx: ctx}
	return context.WithValue(ctx, writeEvidenceQueryKey{}, writeEvidenceQuery{intent, permit}), permit.revoke
}

func VerifyWriteEvidenceRead(ctx context.Context, intent WriteIntent) error {
	query, ok := ctx.Value(writeEvidenceQueryKey{}).(writeEvidenceQuery)
	if !ok || !query.permit.active() || !query.intent.Matches(intent) {
		return ErrAccessDenied
	}
	if !time.Now().Before(intent.RetainUntil) {
		return ErrWriteEvidenceUnavailable
	}
	return ctx.Err()
}
