package superagent_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shubolab/superagent"
)

const outputTool superagent.ToolRef = "capture"

func artifactFixture(t *testing.T, model superagent.ModelProvider, tool superagent.Tool) (superagent.MeshConfig, superagent.ArtifactStore, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := superagent.NewLocalArtifactStore(superagent.LocalArtifactConfig{
		Directory: dir, MaxArtifactBytes: 128 << 10, MaxStoredBytes: 256 << 10,
		MaxArtifacts: 4, MaxPageBytes: 4096, Retention: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cfg := hostConfig(model)
	cfg.LocalArtifacts = store
	cfg.LocalTools = map[superagent.ToolRef]superagent.LocalToolBinding{outputTool: {
		Tool: tool, Spec: superagent.ToolSpec{InputSchema: json.RawMessage(`{}`), NeedsArtifactOutput: true,
			Limits: superagent.ToolLimits{MaxRawOutputBytes: 128 << 10, MaxResultBytes: 2048}},
	}}
	rc := cfg.LocalRunners["main"]
	rc.DefaultBudget.MaxModelRequests = pointer(uint64(2))
	rc.DefaultBudget.MaxToolRequests = pointer(uint64(2))
	rc.DefaultBudget.MaxToolRawOutputBytes = pointer(uint64(192 << 10))
	rc.DefaultBudget.MaxOutputBytes = pointer(uint64(8192))
	rc.BudgetCaps = rc.DefaultBudget
	rc.AllowedTools = map[superagent.SubjectID]map[superagent.ToolRef]superagent.ToolPermission{
		"alice": {outputTool: {AllowArtifacts: true}},
	}
	cfg.LocalRunners["main"] = rc
	return cfg, store, dir
}

func artifactRequest(id string) superagent.StartRequest {
	req := request(superagent.RunID(id))
	req.Tools = []superagent.ToolRef{outputTool}
	return req
}

func artifactCall(ids ...superagent.CallID) superagent.ModelEvent {
	var calls []superagent.ModelToolCall
	for _, id := range ids {
		calls = append(calls, superagent.ModelToolCall{Tool: outputTool, CallID: id, Arguments: json.RawMessage(`{}`)})
	}
	return superagent.ModelEvent{Kind: superagent.ModelCompleted,
		Reply: &superagent.ModelReply{Finish: superagent.ModelFinishToolCalls, ToolCalls: calls}}
}

func artifactReceipt(req superagent.ToolRequest, ref superagent.ArtifactRef) superagent.ToolResult {
	result := dataReceipt(req.RunID, req.CallID, "captured")
	result.Completion.Artifacts = []superagent.ArtifactRef{ref}
	return result
}

func TestToolArtifactsStreamAndSettleIndependentOutput(t *testing.T) {
	ctx := hostContext(t, "alice")
	type staged struct {
		request superagent.ToolRequest
		ref     superagent.ArtifactRef
	}
	ready := make(chan staged, 2)
	release := make(chan struct{})
	model := modelFunc(func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
		if req.Step == 1 {
			return scripted(artifactCall("first", "second")), nil
		}
		if len(req.Messages) != 4 || len(req.Messages[2].Tool.Result.Completion.Artifacts) != 1 ||
			req.Messages[2].Tool.Result.Completion.Content != "captured" || req.MaxOutputBytes < 6000 {
			t.Error("large artifact bytes entered the model's shared output")
		}
		return scripted(completed("done")), nil
	})
	writer := compactTool(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
		tampered := req
		tampered.CallID = "another-call"
		if _, err := superagent.OpenToolArtifact(ctx, tampered); !errors.Is(err, superagent.ErrAccessDenied) {
			return superagent.ToolResult{}, fmt.Errorf("cross-call writer: %v", err)
		}
		w, err := superagent.OpenToolArtifact(ctx, req)
		if err != nil {
			return superagent.ToolResult{}, err
		}
		defer w.Close()
		chunk := bytes.Repeat([]byte("a"), 4096)
		blocks := 24
		if req.CallID == "second" {
			blocks = 20
		}
		for range blocks {
			if n, err := w.Write(chunk); n != len(chunk) || err != nil {
				return superagent.ToolResult{}, fmt.Errorf("write %d bytes: %w", n, err)
			}
		}
		ref, err := w.Complete()
		if err != nil {
			return superagent.ToolResult{}, err
		}
		ready <- staged{request: req, ref: ref}
		if req.CallID == "first" {
			select {
			case <-release:
			case <-ctx.Done():
				return superagent.ToolResult{}, ctx.Err()
			}
		}
		return artifactReceipt(req, ref), nil
	})
	cfg, _, _ := artifactFixture(t, model, writer)
	client := newClient(t, cfg)
	if _, err := client.Start(hostContext(t, "bob"), artifactRequest("denied")); !errors.Is(err, superagent.ErrAccessDenied) {
		t.Fatalf("unauthorized artifact Run = %v", err)
	}
	run := start(t, ctx, client, artifactRequest("stream"))
	defer run.Cancel(ctx)
	first := recv(t, ctx, ready)
	if first.request.Limits.MaxRawOutputBytes != 128<<10 || first.ref.SizeBytes != 96<<10 {
		t.Fatalf("first staged item = %+v", first)
	}
	if _, err := run.ReadArtifact(ctx, first.ref, 0, 16); !errors.Is(err, superagent.ErrArtifactUnavailable) {
		t.Fatalf("staged data exposed before receipt: %v", err)
	}
	close(release)
	second := recv(t, ctx, ready)
	if second.request.Limits.MaxRawOutputBytes != 96<<10 || second.ref.SizeBytes != 80<<10 {
		t.Fatalf("actual first output did not reduce second quota: %+v", second)
	}
	result := wait(t, ctx, run, superagent.Succeeded)
	if result.Cleanup != superagent.CleanupReleased || len(result.Messages) != 5 ||
		*result.Messages[2].Tool.Result.Usage.RawOutputBytes != 96<<10 || *result.Messages[3].Tool.Result.Usage.RawOutputBytes != 80<<10 {
		t.Fatalf("result did not attribute actual writes: %+v", result)
	}
	hash := sha256.New()
	for offset := uint64(0); offset < first.ref.SizeBytes; {
		page, err := run.ReadArtifact(ctx, first.ref, offset, 1<<20)
		if err != nil || len(page.Content) != 4096 || page.NextOffset != offset+4096 ||
			page.EOF != (page.NextOffset == first.ref.SizeBytes) {
			t.Fatalf("artifact page %+v, %v", page, err)
		}
		_, _ = hash.Write(page.Content)
		offset = page.NextOffset
	}
	if hex.EncodeToString(hash.Sum(nil)) != first.ref.SHA256 {
		t.Fatal("artifact content did not match declared digest")
	}
	tampered := first.ref
	tampered.Version = "other"
	for _, attempt := range []struct {
		r   *superagent.Run
		ctx context.Context
		ref superagent.ArtifactRef
	}{
		{run, ctx, tampered}, {client.Run("other"), ctx, first.ref}, {run, hostContext(t, "bob"), first.ref},
	} {
		if _, err := attempt.r.ReadArtifact(attempt.ctx, attempt.ref, 0, 7); !errors.Is(err, superagent.ErrArtifactUnavailable) {
			t.Fatalf("invalid read succeeded: %v", err)
		}
	}
}

func TestToolArtifactPartialOutputsAreDiscarded(t *testing.T) {
	for _, mode := range []string{"overflow", "cancel", "lost", "forged"} {
		t.Run(mode, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			entered := make(chan superagent.ArtifactRef, 1)
			writer := compactTool(func(ctx context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
				w, err := superagent.OpenToolArtifact(ctx, req)
				if err != nil {
					return superagent.ToolResult{}, err
				}
				defer w.Close()
				if n, err := w.Write(bytes.Repeat([]byte("x"), 8)); mode == "overflow" {
					if n != 5 || !errors.Is(err, superagent.ErrArtifactOutputLimit) {
						t.Errorf("overflow wrote %d, %v", n, err)
					}
					if _, err := w.Complete(); err == nil {
						t.Error("limited output became a complete artifact")
					}
					return superagent.ToolResult{RunID: req.RunID, CallID: req.CallID, State: superagent.ToolExecutionKnown,
						Completion: &superagent.ToolCompletion{Outcome: superagent.ToolFailed},
						Reason:     &superagent.ToolReason{Code: "artifact_output_limit"}}, nil
				} else if n != 8 || err != nil {
					return superagent.ToolResult{}, fmt.Errorf("test write: %w", err)
				}
				if mode == "cancel" {
					entered <- superagent.ArtifactRef{}
					<-ctx.Done()
					return superagent.ToolResult{RunID: req.RunID, CallID: req.CallID, State: superagent.ToolExecutionKnown,
						Completion: &superagent.ToolCompletion{Outcome: superagent.ToolCancelled},
						Reason:     &superagent.ToolReason{Code: "cancelled"}}, nil
				}
				ref, err := w.Complete()
				if err != nil {
					return superagent.ToolResult{}, err
				}
				entered <- ref
				if mode == "forged" {
					ref.SizeBytes++
					return artifactReceipt(req, ref), nil
				}
				return superagent.ToolResult{}, errors.New("receipt lost")
			})
			model := modelFunc(func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
				if req.Step == 1 {
					return scripted(artifactCall("first")), nil
				}
				return scripted(completed("done")), nil
			})
			cfg, _, dir := artifactFixture(t, model, writer)
			req := artifactRequest("output")
			if mode == "overflow" {
				req.Budget.MaxToolRawOutputBytes = pointer(uint64(5))
			}
			run := start(t, ctx, newClient(t, cfg), req)
			switch mode {
			case "overflow":
				result := wait(t, ctx, run, superagent.Succeeded)
				r := result.Messages[2].Tool.Result
				if len(r.Completion.Artifacts) != 0 || *r.Usage.RawOutputBytes != 5 || r.Reason.Code != "artifact_output_limit" {
					t.Fatalf("overflow receipt %+v", r)
				}
			case "cancel":
				recv(t, ctx, entered)
				if err := run.Cancel(ctx); err != nil {
					t.Fatal(err)
				}
				wait(t, ctx, run, superagent.Cancelled)
			default:
				ref := recv(t, ctx, entered)
				wait(t, ctx, run, superagent.ToolOutcomeUnknown)
				if _, err := run.ReadArtifact(ctx, ref, 0, 8); !errors.Is(err, superagent.ErrArtifactUnavailable) {
					t.Fatalf("unconfirmed data remained readable: %v", err)
				}
			}
			dirs, err := os.ReadDir(dir)
			if err != nil || len(dirs) != 1 {
				t.Fatalf("artifact directories: %v, %v", dirs, err)
			}
			files, err := os.ReadDir(filepath.Join(dir, dirs[0].Name()))
			if err != nil || len(files) != 0 {
				t.Fatalf("partial files remained: %v, %v", files, err)
			}
		})
	}
}
