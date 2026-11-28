package sdk

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func requireLocalRecovery(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("local process locking is supported on macOS and Linux")
	}
}

func TestRecoveryAdmissionAndOwnership(t *testing.T) {
	requireLocalRecovery(t)
	t.Run("checkpoint-write-failure", func(t *testing.T) {
		root := makeRecoveryRoot(t)
		client, req := fixtureClient(t, defaultRecoveryFixture(root))
		runDir := filepath.Join(root, "runs")
		if err := os.Remove(runDir); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(runDir, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Start(t.Context(), req); !errors.Is(err, contract.ErrRecoveryStorage) {
			t.Fatalf("Start = %v", err)
		}
		if len(readRecoveryLog[contract.ModelRequest](t, root, "models")) != 0 ||
			len(readRecoveryLog[contract.ReceiptKey](t, root, "invoked")) != 0 {
			t.Fatal("failed initial checkpoint resulted in a dispatch")
		}
	})
	t.Run("duplicate-and-recovery-owner", func(t *testing.T) {
		root := makeRecoveryRoot(t)
		kill := recoveryProcess(t, root, "start", "initial")
		fixture := defaultRecoveryFixture(root)
		client, req := fixtureClient(t, fixture)
		if handle, err := client.Start(t.Context(), req); err != nil || handle.ID() != req.ID {
			t.Fatalf("duplicate Start = %v, %v", handle, err)
		}
		conflict := req
		conflict.Input.Text = "changed"
		if _, err := client.Start(t.Context(), conflict); !errors.Is(err, contract.ErrStartConflict) {
			t.Fatalf("conflicting Start = %v", err)
		}
		if _, err := client.Recover(t.Context(), req.ID); !errors.Is(err, contract.ErrRecoveryBusy) {
			t.Fatalf("Recover while owned = %v", err)
		}
		kill()
		other := defaultRecoveryFixture(root)
		other.subject = "other"
		wrongSubject, _ := fixtureClient(t, other)
		if _, err := wrongSubject.Recover(t.Context(), req.ID); !errors.Is(err, contract.ErrRunNotFound) {
			t.Fatalf("Recover from another subject = %v", err)
		}
		other.subject, other.version = "owner", "v2"
		wrongVersion, _ := fixtureClient(t, other)
		if _, err := wrongVersion.Recover(t.Context(), req.ID); !errors.Is(err, contract.ErrRecoveryBinding) {
			t.Fatalf("Recover with another binding = %v", err)
		}
		if len(readRecoveryLog[contract.ModelRequest](t, root, "models")) != 0 {
			t.Fatal("rejected requests dispatched a model")
		}
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				handle, err := client.Recover(t.Context(), req.ID)
				if err != nil {
					t.Error(err)
					return
				}
				result, err := handle.Wait(t.Context())
				if err != nil || result.Outcome != contract.Succeeded {
					t.Errorf("shared recovery = %+v (%v)", result, err)
				}
			}()
		}
		wg.Wait()
		if len(readRecoveryLog[contract.ModelRequest](t, root, "models")) != 2 ||
			len(readRecoveryLog[contract.ReceiptKey](t, root, "invoked")) != 1 {
			t.Fatal("concurrent recovery dispatched a duplicate")
		}
	})
}

func TestRecoveryWaitAndCancel(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	kill := recoveryProcess(t, root, "start", "before-delivery", "SUPERAGENT_RECOVERY_TEST_LONG_WINDOW=1")
	kill()
	client, req := fixtureClient(t, defaultRecoveryFixture(root))
	handle, err := client.Recover(t.Context(), req.ID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if _, err := handle.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed Wait = %v", err)
	}
	if err := handle.Cancel(t.Context()); err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(t.Context())
	if err != nil || result.Outcome != contract.ToolOutcomeUnknown || result.Stop.Trigger != contract.StopCancelled || result.Stop.CallID != "c1" {
		t.Fatalf("Cancel result = %+v (%v)", result, err)
	}
	fresh, _ := fixtureClient(t, defaultRecoveryFixture(root))
	again, err := fresh.Recover(t.Context(), req.ID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := again.Wait(t.Context())
	if err != nil || !reflect.DeepEqual(result, second) || len(readRecoveryLog[contract.ReceiptKey](t, root, "invoked")) != 0 {
		t.Fatalf("cancelled terminal changed: %+v (%v)", second, err)
	}
}

func TestRecoveryReconcileDeadlineSurvivesProcess(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	flag := "SUPERAGENT_RECOVERY_TEST_LONG_WINDOW=1"
	kill := recoveryProcess(t, root, "start", "before-delivery", flag)
	kill()
	kill = recoveryProcess(t, root, "recover", "reconciling", flag)
	before := readRecoveryCheckpoint(t, root)
	kill()
	recoveryProcess(t, root, "recover", "", flag)
	after := readRecoveryCheckpoint(t, root)
	results := readRecoveryLog[contract.Result](t, root, "results")
	if before.ReconcileUntil.IsZero() || !before.ReconcileUntil.Equal(after.ReconcileUntil) ||
		!before.Deadline.Equal(after.Deadline) || len(results) != 1 || results[0].Outcome != contract.ToolOutcomeUnknown ||
		len(readRecoveryLog[contract.ReceiptKey](t, root, "invoked")) != 0 {
		t.Fatalf("restart changed reconciliation: before %+v after %+v, results %+v", before, after, results)
	}
}

func TestRecoveryRejectsUntrustedReceiptsAndRechecksGates(t *testing.T) {
	requireLocalRecovery(t)
	for _, tc := range []struct {
		name, barrier string
		flags         []string
		configure     func(*recoveryFixture)
		want          contract.RunOutcome
		stop          contract.StopCode
		writes        int
	}{
		{name: "mismatched-receipt", barrier: "after-receipt", configure: func(f *recoveryFixture) { f.mismatchedReceipt = true },
			want: contract.ToolOutcomeUnknown, stop: contract.StopToolUnknown, writes: 1},
		{name: "known-not-executed", barrier: "after-receipt", flags: []string{"SUPERAGENT_RECOVERY_TEST_UNEXECUTED=1"}, want: contract.Succeeded},
		{name: "no-model-budget", barrier: "after-receipt", flags: []string{"SUPERAGENT_RECOVERY_TEST_SINGLE_MODEL=1"},
			want: contract.BudgetExceeded, stop: contract.StopModelRequestLimit, writes: 1},
		{name: "workspace-unavailable", barrier: "after-receipt", configure: func(f *recoveryFixture) { f.blockWorkspace = true },
			want: contract.Failed, stop: contract.StopWorkspaceUnavailable, writes: 1},
		{name: "permission-narrowed", barrier: "before-tool-intent", configure: func(f *recoveryFixture) { f.prefix = "other" },
			want: contract.Succeeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := makeRecoveryRoot(t)
			kill := recoveryProcess(t, root, "start", tc.barrier, tc.flags...)
			kill()
			fixture := defaultRecoveryFixture(root)
			if tc.configure != nil {
				tc.configure(fixture)
			}
			client, req := fixtureClient(t, fixture)
			handle, err := client.Recover(t.Context(), req.ID)
			if err != nil {
				t.Fatal(err)
			}
			result, err := handle.Wait(t.Context())
			if err != nil || result.Outcome != tc.want || (tc.stop != "" && (result.Stop == nil || result.Stop.Code != tc.stop)) {
				t.Fatalf("recovery result = %+v (%v)", result, err)
			}
			data, err := os.ReadFile(filepath.Join(root, "workspace", "notes.txt"))
			if (err != nil && !errors.Is(err, os.ErrNotExist)) || strings.Count(string(data), "saved:c1\n") != tc.writes {
				t.Fatalf("file writes = %q (%v)", data, err)
			}
			models := readRecoveryLog[contract.ModelRequest](t, root, "models")
			if tc.want != contract.Succeeded {
				if len(models) != 1 {
					t.Fatal("stop condition allowed an additional model call")
				}
			} else {
				tool := models[1].Messages[2].Tool.Result
				want := "already_fenced"
				if tc.name == "permission-narrowed" {
					want = "path_not_authorized"
				}
				if tool.State != contract.ToolExecutionNotExecuted || tool.Reason.Code != want {
					t.Fatalf("expected a paired failure: %+v", tool)
				}
			}
		})
	}
}

func TestLocalReceiptJournalDeduplicates(t *testing.T) {
	requireLocalRecovery(t)
	calls := 0
	tool, err := NewLocalReceiptTool(t.TempDir(), deliveryTool(func(_ context.Context, req contract.ToolRequest) (contract.ToolResult, error) {
		calls++
		return contract.ToolResult{RunID: req.RunID, CallID: req.CallID, State: contract.ToolExecutionKnown,
			Completion: &contract.ToolCompletion{Outcome: contract.ToolSucceeded, Content: "original"}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	key := contract.ReceiptKey{Runner: "runner", Subject: "owner", RunID: "run", Step: 1, CallID: "call", Tool: "tool", BindingVersion: "v1", Fingerprint: "original"}
	req := contract.ToolRequest{RunID: key.RunID, CallID: key.CallID, Tool: key.Tool, Limits: contract.ToolLimits{MaxResultBytes: 1024}}
	ctx := contract.WithReceiptKey(t.Context(), key)
	original, err := tool.Invoke(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := tool.Invoke(ctx, req)
	if err != nil || !reflect.DeepEqual(original, repeat) || calls != 1 {
		t.Fatalf("duplicate invoke = %+v (%v), %d calls", repeat, err, calls)
	}
	changed := key
	changed.Fingerprint = "changed"
	if _, err := tool.Invoke(contract.WithReceiptKey(t.Context(), changed), req); !errors.Is(err, contract.ErrStartConflict) {
		t.Fatalf("invoke with changed key = %v", err)
	}
	provider := tool.(contract.ReceiptProvider)
	if _, err := provider.QueryReceipt(t.Context(), changed); !errors.Is(err, contract.ErrAccessDenied) {
		t.Fatalf("query with changed key = %v", err)
	}
	if got, err := provider.QueryReceipt(t.Context(), key); err != nil || got == nil || got.Key != key || !reflect.DeepEqual(got.Result, original) {
		t.Fatalf("original receipt = %+v (%v)", got, err)
	}
}

var retainedPayload = strings.Repeat("stable payload across local processes\n", 128)

func TestRecoveryRetainsArtifactReferenceAcrossProcesses(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	kill := recoveryProcess(t, root, "start", "after-receipt", "SUPERAGENT_RECOVERY_TEST_ARTIFACT=1")
	kill()
	fixture := defaultRecoveryFixture(root)
	fixture.artifact = true
	client, req := fixtureClient(t, fixture)
	handle, err := client.Recover(t.Context(), req.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := handle.Wait(t.Context())
	if err != nil || result.Outcome != contract.Succeeded {
		t.Fatalf("artifact recovery = %+v (%v)", result, err)
	}
	ref := result.Messages[2].Tool.Result.Completion.Artifacts[0]
	page, err := handle.ReadArtifact(t.Context(), ref, 0, ref.SizeBytes)
	if err != nil || string(page.Content) != retainedPayload || !page.EOF {
		t.Fatalf("retained artifact: %d bytes (%v)", len(page.Content), err)
	}
	for _, pattern := range []string{"runs/*.run", "receipts/*.receipt"} {
		paths, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil || len(paths) != 1 {
			t.Fatalf("records %v (%v)", paths, err)
		}
		data, err := os.ReadFile(paths[0])
		if err != nil || bytes.Contains(data, []byte("stable payload across local processes")) || !bytes.Contains(data, []byte(ref.Version)) {
			t.Fatalf("durable record must store only the artifact reference (%v)", err)
		}
	}
}

func TestRecoveryRejectsChangedWorkspaceAndCorruptCheckpoint(t *testing.T) {
	requireLocalRecovery(t)
	t.Run("replaced-workspace", func(t *testing.T) {
		root := makeRecoveryRoot(t)
		kill := recoveryProcess(t, root, "start", "after-receipt")
		kill()
		work := filepath.Join(root, "workspace")
		old := filepath.Join(root, "original-workspace")
		if err := os.Rename(work, old); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(work, "other"), 0700); err != nil {
			t.Fatal(err)
		}
		client, req := fixtureClient(t, defaultRecoveryFixture(root))
		handle, err := client.Recover(t.Context(), req.ID)
		if err != nil {
			t.Fatal(err)
		}
		result, err := handle.Wait(t.Context())
		if err != nil || result.Outcome != contract.Failed || result.Stop.Code != contract.StopWorkspaceUnavailable {
			t.Fatalf("replaced workspace = %+v (%v)", result, err)
		}
		writes, err := os.ReadFile(filepath.Join(old, "notes.txt"))
		if err != nil || string(writes) != "saved:c1\n" || len(readRecoveryLog[contract.ModelRequest](t, root, "models")) != 1 {
			t.Fatalf("replaced workspace caused a new dispatch: %q (%v)", writes, err)
		}
	})
	t.Run("partial-checkpoint", func(t *testing.T) {
		root := makeRecoveryRoot(t)
		kill := recoveryProcess(t, root, "start", "initial")
		kill()
		files, err := filepath.Glob(filepath.Join(root, "runs", "*.run"))
		if err != nil || len(files) != 1 {
			t.Fatalf("checkpoint files = %v (%v)", files, err)
		}
		if err := os.WriteFile(files[0], []byte(`{"Format":1`), 0600); err != nil {
			t.Fatal(err)
		}
		client, req := fixtureClient(t, defaultRecoveryFixture(root))
		if _, err := client.Recover(t.Context(), req.ID); !errors.Is(err, contract.ErrRecoveryStorage) {
			t.Fatalf("Recover with partial checkpoint = %v", err)
		}
		if len(readRecoveryLog[contract.ModelRequest](t, root, "models")) != 0 {
			t.Fatal("partial checkpoint caused a dispatch")
		}
	})
}

func TestRecoverThroughPublicMesh(t *testing.T) {
	requireLocalRecovery(t)
	var calls int
	budget := contract.Budget{
		Timeout: address(time.Minute), MaxModelRequests: address(uint64(1)),
		MaxModelInputBytes: address(uint64(8192)), MaxOutputBytes: address(uint64(1024)),
	}
	version := "v1"
	dir := t.TempDir()
	newClient := func() *Client {
		m, err := NewMesh(MeshConfig{
			LocalRunners: map[contract.RunnerRef]contract.RunnerConfig{"runner": {
				AllowedModels: map[contract.SubjectID][]contract.ModelRef{"owner": {"model"}},
				DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 4096, ResultRetention: time.Minute,
				Recovery: &contract.RecoveryConfig{Directory: dir, BindingVersion: version, ReconcileTimeout: time.Second},
			}},
			LocalModels: map[contract.ModelRef]contract.ModelProvider{"model": deliveryModel(func(context.Context, contract.ModelRequest) (contract.ModelStream, error) {
				calls++
				return &completionStream{&contract.ModelReply{Finish: contract.ModelFinishStop, Text: "persisted"}}, nil
			})},
			ResolveCaller: func(context.Context) (contract.SubjectID, error) { return "owner", nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		c, err := NewClient(m, "runner")
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	first, err := newClient().Start(t.Context(), contract.StartRequest{ID: recoveryID, Model: "model", Recoverable: true})
	if err != nil {
		t.Fatal(err)
	}
	want, err := first.Wait(t.Context())
	if err != nil || want.Outcome != contract.Succeeded {
		t.Fatalf("first result = %+v (%v)", want, err)
	}
	version = "v2" // A terminal read does not need to open the old model binding.
	handle, err := newClient().Recover(t.Context(), first.ID())
	if err != nil {
		t.Fatal(err)
	}
	got, err := handle.Wait(t.Context())
	if err != nil || !reflect.DeepEqual(want, got) || calls != 1 {
		t.Fatalf("terminal changed: %+v (%v), model calls %d", got, err, calls)
	}
	if _, err := handle.Observe(t.Context()); !errors.Is(err, contract.ErrRecoveryObservation) {
		t.Fatalf("restored observation = %v", err)
	}
}
