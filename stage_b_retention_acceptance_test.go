package superagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func TestStageB5RetainedArtifactFixture(t *testing.T) {
	requireLocalRecovery(t)
	t.Run("paged-and-authorized", func(t *testing.T) {
		root := makeRecoveryRoot(t)
		ref := stageBArtifactReceipt(t, root)
		f := defaultRecoveryFixture(root)
		f.artifact, f.artifactPage = true, 1024
		client, req := fixtureClient(t, f)
		handle, err := client.Recover(t.Context(), req.ID)
		if err != nil {
			t.Fatal(err)
		}
		result, err := handle.Wait(t.Context())
		if err != nil || result.Outcome != Succeeded {
			t.Fatalf("artifact recovery: %+v (%v)", result, err)
		}
		tools := stageBToolResults(t, result.Messages)
		if len(tools) != 1 || tools[0].Completion == nil || len(tools[0].Completion.Artifacts) != 1 || tools[0].Completion.Artifacts[0] != ref {
			t.Fatalf("retained reference changed: %+v", tools)
		}
		var body []byte
		for offset := uint64(0); ; {
			page, err := handle.ReadArtifact(t.Context(), ref, offset, ref.SizeBytes)
			want := min(f.artifactPage, ref.SizeBytes-offset)
			if err != nil || uint64(len(page.Content)) != want || page.NextOffset != offset+want ||
				page.EOF != (page.NextOffset == ref.SizeBytes) {
				t.Fatalf("page at %d: length %d next %d eof %t (%v)", offset, len(page.Content), page.NextOffset, page.EOF, err)
			}
			body = append(body, page.Content...)
			if page.EOF {
				break
			}
			offset = page.NextOffset
		}
		if uint64(len(body)) != ref.SizeBytes || ref.SizeBytes <= 2*f.artifactPage || string(body) != retainedPayload ||
			contract.HashBytes(body) != ref.SHA256 || ref.SHA256 != ref.Version {
			t.Fatalf("retained content/version mismatch: bytes=%d ref=%+v", len(body), ref)
		}
		sizes := make(map[string]int)
		for _, pattern := range []string{"runs/*.run", "receipts/*.receipt"} {
			record, err := os.ReadFile(evidenceFixturePath(t, root, pattern))
			if err != nil || bytes.Contains(record, []byte("stable payload across local processes")) ||
				!bytes.Contains(record, []byte(ref.Version)) || len(record) >= len(body) {
				t.Fatalf("%s did not keep a bounded reference: size=%d artifact=%d (%v)", pattern, len(record), len(body), err)
			}
			sizes[pattern] = len(record)
		}
		other := defaultRecoveryFixture(root)
		other.artifact, other.subject = true, "other"
		otherClient, _ := fixtureClient(t, other)
		expired := defaultRecoveryFixture(root)
		expired.artifact, expired.artifactNow = true, func() time.Time { return ref.ExpiresAt }
		expiredClient, _ := fixtureClient(t, expired)
		tampered := ref
		tampered.SHA256 = contract.HashBytes([]byte("another artifact"))
		for name, attempt := range map[string]struct {
			run *Run
			ref ArtifactRef
		}{
			"subject": {otherClient.Run(req.ID), ref},
			"run":     {client.Run("other-run"), ref},
			"hash":    {handle, tampered},
			"expiry":  {expiredClient.Run(req.ID), ref},
		} {
			if _, err := attempt.run.ReadArtifact(t.Context(), attempt.ref, 0, 32); !errors.Is(err, ErrArtifactUnavailable) {
				t.Fatalf("%s read should be unavailable: %v", name, err)
			}
		}
		if len(readRecoveryLog[ReceiptKey](t, root, "invoked")) != 1 || len(readRecoveryLog[ModelRequest](t, root, "models")) != 2 {
			t.Fatal("artifact recovery repeated original work")
		}
		t.Logf("fixture=retainedFixtureStore writer=separate-process artifact=%d page_cap=%d sha256=%s checkpoint=%d receipt=%d",
			len(body), f.artifactPage, ref.SHA256, sizes["runs/*.run"], sizes["receipts/*.receipt"])
	})
	t.Run("missing", func(t *testing.T) {
		root := makeRecoveryRoot(t)
		ref := stageBArtifactReceipt(t, root)
		store := retainedFixtureStore{root: root}
		path := store.path(ArtifactOwner{Runner: "runner", Subject: "owner", RunID: recoveryID}, ref)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		f := defaultRecoveryFixture(root)
		f.artifact, f.workspaceWait = true, 300*time.Millisecond
		client, req := fixtureClient(t, f)
		handle, err := client.Recover(t.Context(), req.ID)
		if err != nil {
			t.Fatal(err)
		}
		result, err := handle.Wait(t.Context())
		if err != nil || result.Outcome != ToolOutcomeUnknown || result.Stop == nil || result.Stop.CallID != "c1" ||
			result.Stop.Trigger != StopReconcileDeadline || len(stageBToolResults(t, result.Messages)) != 0 ||
			len(readRecoveryLog[ReceiptKey](t, root, "invoked")) != 1 || len(readRecoveryLog[ModelRequest](t, root, "models")) != 1 {
			t.Fatalf("missing artifact produced a replay or continuation: %+v (%v)", result, err)
		}
		t.Logf("fixture=retainedFixtureStore missing artifact, outcome=%s, dependent dispatches=0 resends=0", result.Outcome)
	})
}

func stageBArtifactReceipt(t *testing.T, root string) ArtifactRef {
	t.Helper()
	crash := recoveryProcess(t, root, "start", "after-receipt", "SUPERAGENT_RECOVERY_TEST_ARTIFACT=1")
	crash()
	raw, err := os.ReadFile(evidenceFixturePath(t, root, "receipts/*.receipt"))
	var record struct {
		Result *ToolResult
	}
	cp := readRecoveryCheckpoint(t, root)
	if err != nil || json.Unmarshal(raw, &record) != nil || record.Result == nil || record.Result.Completion == nil ||
		len(record.Result.Completion.Artifacts) != 1 || cp.Intent == nil || cp.Intent.Key.CallID != record.Result.CallID {
		t.Fatalf("writer did not exit with a durable receipt and pending checkpoint: %q (%v)", raw, err)
	}
	return record.Result.Completion.Artifacts[0]
}

type stageBLifecycleConfig struct {
	Root, Phase string
	Retention   time.Duration
}

func TestStageBLifecycleProcess(t *testing.T) {
	raw := os.Getenv("SUPERAGENT_STAGE_B_LIFECYCLE")
	if raw == "" {
		return
	}
	var cfg stageBLifecycleConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	f := defaultRecoveryFixture(cfg.Root)
	f.resultRetention = cfg.Retention
	client, req := fixtureClient(t, f)
	var handle *Run
	var err error
	if cfg.Phase == "start" {
		handle, err = client.Start(t.Context(), req)
	} else {
		handle, err = client.Recover(t.Context(), req.ID)
	}
	if cfg.Phase == "expired" {
		if !errors.Is(err, ErrRunNotFound) || handle != nil {
			t.Fatalf("expired recovery: handle=%v err=%v", handle, err)
		}
		f.append("stage-b-retention-expired", os.Getpid())
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(t.Context())
	if err != nil || result.Outcome != Succeeded {
		t.Fatalf("%s Wait: %+v (%v)", cfg.Phase, result, err)
	}
	f.append("stage-b-retention-"+cfg.Phase, result)
}

func TestStageB6TerminalRetentionAcrossProcesses(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	const retention = 4 * time.Second
	launch := func(phase string) stageBProcess {
		cfg := stageBLifecycleConfig{Root: root, Phase: phase, Retention: retention}
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return launchStageBProcess(t, "TestStageBLifecycleProcess", "SUPERAGENT_STAGE_B_LIFECYCLE="+string(raw))
	}
	first := launch("start")
	first.wait()
	cp := readRecoveryCheckpoint(t, root)
	cutoff := cp.FinishedAt.Add(retention)
	if cp.Result == nil || !time.Now().Before(cutoff) {
		t.Fatalf("run not retained at the first restart: %+v", cp)
	}
	inside := launch("inside")
	inside.wait()
	got := readRecoveryLog[Result](t, root, "stage-b-retention-inside")
	if len(got) != 1 || !reflect.DeepEqual(got[0], *cp.Result) {
		t.Fatalf("in-window read changed result: %+v", got)
	}
	delay := time.NewTimer(time.Until(cutoff) + 10*time.Millisecond)
	defer delay.Stop()
	select {
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	case <-delay.C:
	}
	outside := launch("expired")
	outside.wait()
	pids := readRecoveryLog[int](t, root, "stage-b-retention-expired")
	if !reflect.DeepEqual(pids, []int{outside.pid}) ||
		!reflect.DeepEqual(readRecoveryCheckpoint(t, root), cp) ||
		len(readRecoveryLog[ModelRequest](t, root, "models")) != 2 ||
		len(readRecoveryLog[ReceiptKey](t, root, "invoked")) != 1 ||
		len(readRecoveryLog[ReceiptKey](t, root, "queries")) != 0 {
		t.Fatal("terminal reads produced new work or updated the original checkpoint")
	}
	t.Logf("start pid=%d in-window pid=%d expired pid=%d retention=%s after_expiry=ErrRunNotFound new_dispatches=0", first.pid, inside.pid, outside.pid, retention)
}

func TestStageB6UnsupportedBindingsRejectBeforeDispatch(t *testing.T) {
	requireLocalRecovery(t)
	for _, kind := range []string{"builtin-artifact", "compaction"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			var models, tools atomic.Int32
			budget := Budget{Timeout: address(time.Second), MaxModelRequests: address(uint64(2)), MaxModelInputBytes: address(uint64(8192)),
				MaxOutputBytes: address(uint64(8192)), MaxToolRequests: address(uint64(1)), MaxToolRawOutputBytes: address(uint64(8192))}
			rc := RunnerConfig{AllowedModels: map[SubjectID][]ModelRef{"owner": {"model"}},
				DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 1024, ResultRetention: time.Minute,
				Recovery: &RecoveryConfig{Directory: filepath.Join(root, "runs"), BindingVersion: "v1", ReconcileTimeout: time.Second}}
			host := MeshConfig{
				LocalModels: map[ModelRef]ModelProvider{"model": deliveryModel(func(context.Context, ModelRequest) (ModelStream, error) {
					models.Add(1)
					return &completionStream{&ModelReply{Finish: ModelFinishStop, Text: "ready"}}, nil
				})},
				ResolveCaller: func(context.Context) (SubjectID, error) { return "owner", nil },
			}
			const ref ToolRef = "output"
			if kind == "builtin-artifact" {
				store, err := NewLocalArtifactStore(LocalArtifactConfig{Directory: root, MaxArtifactBytes: 8192, MaxStoredBytes: 8192,
					MaxArtifacts: 2, MaxPageBytes: 512, Retention: time.Minute})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				host.LocalArtifacts = store
				tool, err := NewLocalReceiptTool(filepath.Join(root, "receipts"), deliveryTool(func(context.Context, ToolRequest) (ToolResult, error) {
					tools.Add(1)
					return ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				host.LocalTools = map[ToolRef]LocalToolBinding{ref: {Tool: tool, Spec: ToolSpec{NeedsArtifactOutput: true,
					InputSchema: json.RawMessage(`{}`), Limits: ToolLimits{MaxRawOutputBytes: 8192, MaxResultBytes: 1024}}}}
				rc.AllowedTools = map[SubjectID]map[ToolRef]ToolPermission{"owner": {ref: {AllowArtifacts: true}}}
			} else {
				host.LocalTools = map[ToolRef]LocalToolBinding{ref: NewContextSourceTool()}
				rc.AllowedTools = map[SubjectID]map[ToolRef]ToolPermission{"owner": {ref: {}}}
				rc.ContextPolicies = map[ModelRef]ModelContextPolicy{"model": {MaxOutputTokens: 20, Compaction: &ContextCompactionPolicy{
					TriggerRatio: .75, TargetRatio: .60, SummaryMaxTokens: 16, SummaryMaxBytes: 256, MinSavedTokens: 10,
					MaxCompactionsPerRun: 3, SourcePageBytes: 32, SourceTool: ref,
				}}}
			}
			host.LocalRunners = map[RunnerRef]RunnerConfig{"runner": rc}
			m, err := NewMesh(host)
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(m, "runner")
			if err != nil {
				t.Fatal(err)
			}
			if handle, err := client.Start(t.Context(), StartRequest{ID: "unsupported", Model: "model", Tools: []ToolRef{ref}, Recoverable: true}); handle != nil || !errors.Is(err, ErrRecoveryUnavailable) {
				t.Fatalf("%s start: handle=%v err=%v", kind, handle, err)
			}
			files, err := filepath.Glob(filepath.Join(root, "runs", "*.run"))
			if err != nil || len(files) != 0 || models.Load() != 0 || tools.Load() != 0 {
				t.Fatalf("unsupported binding admitted work: runs=%v models=%d tools=%d (%v)", files, models.Load(), tools.Load(), err)
			}
			t.Logf("binding=%s error=ErrRecoveryUnavailable checkpoints=0 models=0 tools=0", kind)
		})
	}
}
