package environment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func TestTakeoverWorkspaceRequiresFencedPublication(t *testing.T) {
	if !supportsRootedRead {
		t.Skip("requires local rooted workspace")
	}
	root := t.TempDir()
	env, err := NewTakeoverLocal(map[contract.WorkspaceRef]WorkspaceConfig{"ws": {Root: root, ReadPrefixes: []string{"."}}})
	if err != nil {
		t.Fatal(err)
	}
	scope := contract.RunScope{Runner: "runner", Subject: "owner", RunID: "run", Workspace: "ws", Epoch: 1, Deadline: time.Now().Add(time.Minute)}
	handle, err := env.Allocate(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Release(context.Background(), scope, handle) })
	call := contract.CallScope{Run: scope, Handle: handle, CallID: "c1", Tool: "write", Path: "notes.txt",
		Limits: contract.ToolLimits{MaxScanBytes: 8, MaxResultBytes: 256}}
	grant, err := env.Grant(t.Context(), call, ".")
	if err != nil {
		t.Fatal(err)
	}
	if err := env.PublishWorkspace(t.Context(), call, grant, []byte("bypass")); !errors.Is(err, contract.ErrAccessDenied) {
		t.Fatalf("unfenced publication = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, call.Path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unfenced publication modified the workspace: %v", err)
	}
}
