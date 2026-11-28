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

	stage := func(scope contract.CallScope, grant contract.EnvironmentGrantRef, intent contract.WriteIntent, data string) (contract.StagedWrite, error) {
		writeCtx, end := contract.WithWorkspaceWriteAccess(ctx, scope, grant, intent, nil)
		defer end()
		return workspace.StageWrite(writeCtx, scope, grant, intent, []byte(data))
	}
	commit := func(scope contract.CallScope, grant contract.EnvironmentGrantRef, intent contract.WriteIntent, staged contract.StagedWrite) (contract.WriteEvidence, error) {
		commitCtx, end := contract.WithWorkspaceWriteAccess(ctx, scope, grant, intent, &staged)
		defer end()
		return workspace.CommitWrite(commitCtx, scope, grant, intent, staged)
	}
	cs, grant, first := prepare("c1", "first\n")
	staged, err := stage(cs, grant, first, "first\n")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.OpenRead(ctx, cs, grant, cs.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged data is visible: %v", err)
	}
	query, endQuery := contract.WithWriteEvidenceQuery(ctx, first)
	defer endQuery()
	if _, err := workspace.ReadWriteEvidence(query, first); !errors.Is(err, contract.ErrWriteEvidenceUnavailable) {
		t.Fatalf("staged operation has proof: %v", err)
	}
	proof, err := commit(cs, grant, first, staged)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := stage(cs, grant, first, "first\n")
	if err != nil || repeated != staged {
		t.Fatalf("repeat staged a different version: %+v (%v)", repeated, err)
	}
	if duplicate, err := commit(cs, grant, first, repeated); err != nil || duplicate.Ref != proof.Ref {
		t.Fatalf("repeat committed a different proof: %+v (%v)", duplicate, err)
	}
	conflict := first
	conflict.AppendHash, conflict.AppendBytes = contract.HashBytes([]byte("changed")), 7
	if _, err := stage(cs, grant, conflict, "changed"); !errors.Is(err, contract.ErrStartConflict) {
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
	next, err := stage(secondScope, secondGrant, second, "second\n")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := commit(secondScope, secondGrant, second, next); err != nil {
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
