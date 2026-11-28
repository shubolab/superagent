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

type EvidenceWorkspace interface {
	TakeoverWorkspace
	StageWrite(context.Context, CallScope, EnvironmentGrantRef, WriteIntent, []byte) (StagedWrite, error)
	CommitWrite(context.Context, CallScope, EnvironmentGrantRef, WriteIntent, StagedWrite) (WriteEvidence, error)
	ReadWriteEvidence(context.Context, WriteIntent) (WriteEvidence, error)
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

func WithWriteEvidenceQuery(ctx context.Context, intent WriteIntent) context.Context {
	return context.WithValue(ctx, writeEvidenceQueryKey{}, intent)
}

func HasWriteEvidenceQuery(ctx context.Context, intent WriteIntent) bool {
	authorized, ok := ctx.Value(writeEvidenceQueryKey{}).(WriteIntent)
	return ok && authorized.Matches(intent)
}
