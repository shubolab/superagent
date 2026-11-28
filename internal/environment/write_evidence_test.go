package environment

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

func TestManagedWriteEvidenceRetentionAndConflicts(t *testing.T) {
	if !durable.Supported {
		t.Skip("requires durable local locks")
	}
	root := t.TempDir()
	config := EvidenceWorkspaceConfig{Directory: filepath.Join(t.TempDir(), "managed"), MaxFileBytes: 4096}
	workspaces := map[contract.WorkspaceRef]WorkspaceConfig{"project": {Root: root, ReadPrefixes: []string{"."}}}
	workspace, err := NewEvidenceLocal(workspaces, config)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ctx := t.Context()
	run := contract.RunScope{Runner: "runner", Subject: "owner", RunID: "run", Workspace: "project", Deadline: now.Add(time.Minute), Epoch: 1}
	version, err := workspace.RecoveryWorkspace(ctx, run)
	if err != nil {
		t.Fatal(err)
	}

	prepare := func(id contract.CallID, content string) (contract.CallScope, contract.EnvironmentGrantRef, contract.WriteIntent) {
		t.Helper()
		handle, err := workspace.Allocate(ctx, run)
		if err != nil {
			t.Fatal(err)
		}
		cs := contract.CallScope{Run: run, Handle: handle, CallID: id, Tool: "append", Path: "notes.txt",
			Limits: contract.ToolLimits{MaxScanBytes: 128, MaxResultBytes: 1024}}
		grant, err := workspace.Grant(ctx, cs, ".")
		if err != nil {
			t.Fatal(err)
		}
		remote := contract.RemoteToolIdentity{Provider: "provider", Journal: "journal", Version: "v1", Workspace: run.Workspace,
			WorkspaceVersion: version, EvidenceRule: contract.AppendFileEvidenceV1}
		key := contract.ReceiptKey{Runner: run.Runner, Subject: run.Subject, RunID: run.RunID, Step: 1, CallID: id, Tool: cs.Tool,
			BindingVersion: "v1", AdmissionEpoch: run.Epoch, Fingerprint: contract.HashBytes([]byte("request:" + string(id))), Remote: remote}
		intent := contract.WriteIntent{Key: key, Path: cs.Path, AppendHash: contract.HashBytes([]byte(content)),
			AppendBytes: uint64(len(content)), Limits: cs.Limits, RetainUntil: run.Deadline.Add(time.Minute)}
		return cs, grant, intent
	}

	cs, grant, first := prepare("c1", "first\n")
	writeCtx := contract.WithWorkspacePublication(ctx, cs, grant)
	staged, err := workspace.StageWrite(writeCtx, cs, grant, first, []byte("first\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.OpenRead(ctx, cs, grant, cs.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged data is visible: %v", err)
	}
	query := contract.WithWriteEvidenceQuery(ctx, first)
	if _, err := workspace.ReadWriteEvidence(query, first); !errors.Is(err, contract.ErrWriteEvidenceUnavailable) {
		t.Fatalf("staged operation has proof: %v", err)
	}
	proof, err := workspace.CommitWrite(writeCtx, cs, grant, first, staged)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := workspace.StageWrite(writeCtx, cs, grant, first, []byte("first\n"))
	if err != nil || repeated != staged {
		t.Fatalf("repeat staged a different version: %+v (%v)", repeated, err)
	}
	if duplicate, err := workspace.CommitWrite(writeCtx, cs, grant, first, repeated); err != nil || duplicate.Ref != proof.Ref {
		t.Fatalf("repeat committed a different proof: %+v (%v)", duplicate, err)
	}
	conflict := first
	conflict.AppendHash, conflict.AppendBytes = contract.HashBytes([]byte("changed")), 7
	if _, err := workspace.StageWrite(writeCtx, cs, grant, conflict, []byte("changed")); !errors.Is(err, contract.ErrStartConflict) {
		t.Fatalf("conflicting duplicate: %v", err)
	}
	if err := workspace.Release(ctx, run, cs.Handle); err != nil {
		t.Fatal(err)
	}

	workspace, err = NewEvidenceLocal(workspaces, config)
	if err != nil {
		t.Fatal(err)
	}
	secondScope, secondGrant, second := prepare("c2", "second\n")
	secondCtx := contract.WithWorkspacePublication(ctx, secondScope, secondGrant)
	next, err := workspace.StageWrite(secondCtx, secondScope, secondGrant, second, []byte("second\n"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.CommitWrite(secondCtx, secondScope, secondGrant, second, next); err != nil {
		t.Fatal(err)
	}
	f, err := workspace.OpenRead(ctx, secondScope, secondGrant, secondScope.Path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(f)
	_ = f.Close()
	if err != nil || string(data) != "first\nsecond\n" {
		t.Fatalf("current version = %q (%v)", data, err)
	}
	original, err := workspace.ReadWriteEvidence(query, first)
	if err != nil || original.Ref != proof.Ref || original.Version.ID != contract.HashBytes([]byte("first\n")) ||
		original.Version == next.Version {
		t.Fatalf("original version lost after update: %+v (%v)", original, err)
	}
	for _, change := range []func(*contract.WriteIntent){
		func(i *contract.WriteIntent) { i.Key.Subject = "other" },
		func(i *contract.WriteIntent) { i.Key.Remote.Workspace = "other" },
		func(i *contract.WriteIntent) { i.Key.Remote.WorkspaceVersion = "replaced" },
	} {
		modified := first
		change(&modified)
		if _, err := workspace.ReadWriteEvidence(query, modified); !errors.Is(err, contract.ErrAccessDenied) {
			t.Fatalf("query escaped original authorization: %v", err)
		}
	}
	if err := workspace.Release(ctx, run, secondScope.Handle); err != nil {
		t.Fatal(err)
	}
	workspace.now = func() time.Time { return first.RetainUntil }
	if _, err := workspace.ReadWriteEvidence(query, first); !errors.Is(err, contract.ErrWriteEvidenceUnavailable) {
		t.Fatalf("expired evidence was returned: %v", err)
	}
}
