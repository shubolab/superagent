package sdk

import (
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
		if !errors.Is(err, contract.ErrRunNotFound) || handle != nil {
			t.Fatalf("expired recovery: handle=%v err=%v", handle, err)
		}
		f.append("stage-b-retention-expired", os.Getpid())
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(t.Context())
	if err != nil || result.Outcome != contract.Succeeded {
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
	got := readRecoveryLog[contract.Result](t, root, "stage-b-retention-inside")
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
		len(readRecoveryLog[contract.ModelRequest](t, root, "models")) != 2 ||
		len(readRecoveryLog[contract.ReceiptKey](t, root, "invoked")) != 1 ||
		len(readRecoveryLog[contract.ReceiptKey](t, root, "queries")) != 0 {
		t.Fatal("terminal reads produced new work or updated the original checkpoint")
	}
	t.Logf("start pid=%d in-window pid=%d expired pid=%d retention=%s after_expiry=ErrRunNotFound new_dispatches=0", first.pid, inside.pid, outside.pid, retention)
}

type retentionWithoutDurablePublication struct{ contract.TakeoverArtifactStore }

func TestStageB6UnsupportedBindingsRejectBeforeDispatch(t *testing.T) {
	requireLocalRecovery(t)
	for _, kind := range []string{"builtin-artifact", "retained-only-artifact", "compaction"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			var models, tools atomic.Int32
			budget := contract.Budget{Timeout: address(time.Second), MaxModelRequests: address(uint64(2)), MaxModelInputBytes: address(uint64(8192)),
				MaxOutputBytes: address(uint64(8192)), MaxToolRequests: address(uint64(1)), MaxToolRawOutputBytes: address(uint64(8192))}
			rc := contract.RunnerConfig{AllowedModels: map[contract.SubjectID][]contract.ModelRef{"owner": {"model"}},
				DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 1024, ResultRetention: time.Minute,
				Recovery: &contract.RecoveryConfig{Directory: filepath.Join(root, "runs"), BindingVersion: "v1", ReconcileTimeout: time.Second}}
			host := MeshConfig{
				LocalModels: map[contract.ModelRef]contract.ModelProvider{"model": deliveryModel(func(context.Context, contract.ModelRequest) (contract.ModelStream, error) {
					models.Add(1)
					return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "ready"}}, nil
				})},
				ResolveCaller: func(context.Context) (contract.SubjectID, error) { return "owner", nil },
			}
			const ref contract.ToolRef = "output"
			if kind != "compaction" {
				config := LocalArtifactConfig{Directory: root, MaxArtifactBytes: 8192, MaxStoredBytes: 8192,
					MaxArtifacts: 2, MaxPageBytes: 512, Retention: time.Minute}
				newStore := NewLocalArtifactStore
				if kind == "retained-only-artifact" {
					newStore = NewLocalRetainedArtifactStore
					config.Directory = filepath.Join(root, "artifacts")
				}
				store, err := newStore(config)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				host.LocalArtifacts = store
				if kind == "retained-only-artifact" {
					host.LocalArtifacts = retentionWithoutDurablePublication{store.(contract.TakeoverArtifactStore)}
				}
				tool, err := NewLocalReceiptTool(filepath.Join(root, "receipts"), deliveryTool(func(context.Context, contract.ToolRequest) (contract.ToolResult, error) {
					tools.Add(1)
					return contract.ToolResult{}, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				host.LocalTools = map[contract.ToolRef]contract.LocalToolBinding{ref: {Tool: tool, Spec: contract.ToolSpec{NeedsArtifactOutput: true,
					InputSchema: json.RawMessage(`{}`), Limits: contract.ToolLimits{MaxRawOutputBytes: 8192, MaxResultBytes: 1024}}}}
				rc.AllowedTools = map[contract.SubjectID]map[contract.ToolRef]contract.ToolPermission{"owner": {ref: {AllowArtifacts: true}}}
			} else {
				host.LocalTools = map[contract.ToolRef]contract.LocalToolBinding{ref: NewContextSourceTool()}
				rc.AllowedTools = map[contract.SubjectID]map[contract.ToolRef]contract.ToolPermission{"owner": {ref: {}}}
				rc.ContextPolicies = map[contract.ModelRef]contract.ModelContextPolicy{"model": {MaxOutputTokens: 20, Compaction: &contract.ContextCompactionPolicy{
					TriggerRatio: .75, TargetRatio: .60, SummaryMaxTokens: 16, SummaryMaxBytes: 256, MinSavedTokens: 10,
					MaxCompactionsPerRun: 3, SourcePageBytes: 32, SourceTool: ref,
				}}}
			}
			host.LocalRunners = map[contract.RunnerRef]contract.RunnerConfig{"runner": rc}
			m, err := NewMesh(host)
			if err != nil {
				t.Fatal(err)
			}
			client, err := NewClient(m, "runner")
			if err != nil {
				t.Fatal(err)
			}
			if handle, err := client.Start(t.Context(), contract.StartRequest{ID: "unsupported", Model: "model", Tools: []contract.ToolRef{ref}, Recoverable: true}); handle != nil || !errors.Is(err, contract.ErrRecoveryUnavailable) {
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
