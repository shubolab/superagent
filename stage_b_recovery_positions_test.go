package superagent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

func TestStageB2RestartAtRebuiltReceiptCheckpoints(t *testing.T) {
	requireLocalRecovery(t)
	for _, barrier := range []string{"receipt", "first", "batch", "terminal"} {
		t.Run(barrier, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
			defer cancel()
			f := newStageBRemoteFixture(t, false, true, 0, 6*time.Second)
			before := f.crashTool(ctx)
			f.old.stop()
			f.restartTool()
			next := f.recover(ctx, "next", barrier, false)
			f.await(ctx, "stage-b-"+barrier)
			next.stop()

			atCrash := readRecoveryCheckpoint(t, f.root)
			queries := f.proxy.queries.Load()
			expectedCalls, expectedModels := 1, 1
			switch barrier {
			case "receipt":
				delivered := readRecoveryLog[ToolReceipt](t, f.root, "stage-b-delivered-receipts")
				if len(delivered) != 1 || delivered[0].Evidence == nil || *delivered[0].Evidence != f.proof.Ref ||
					atCrash.Intent == nil || atCrash.Intent.Key != before.Intent.Key || len(atCrash.ToolResults) != 0 ||
					len(atCrash.WriteEvidence) != 0 || atCrash.Balance != before.Balance {
					t.Fatalf("crash must precede any durable settlement: %+v, delivered %+v", atCrash, delivered)
				}
			case "first":
				if atCrash.Pending == nil || len(atCrash.Pending.ToolCalls) != 2 || len(atCrash.ToolResults) != 1 || len(atCrash.Messages) != 1 {
					t.Fatalf("crash must leave a partial batch without c2 intent: %+v", atCrash)
				}
				stageBCheckSettled(t, before, atCrash, f.proof, atCrash.ToolResults[0])
			case "batch":
				expectedCalls = 2
				if atCrash.Step != 2 || atCrash.Pending != nil || atCrash.Intent != nil || atCrash.ModelInFlight ||
					atCrash.Result != nil || len(stageBToolResults(t, atCrash.Messages)) != 2 {
					t.Fatalf("crash must follow batch commit and precede the second model intent: %+v", atCrash)
				}
			case "terminal":
				expectedCalls, expectedModels = 2, 2
				if atCrash.Result == nil || atCrash.Result.Outcome != Succeeded || atCrash.FinishedAt.IsZero() ||
					len(readRecoveryLog[Result](t, f.root, "stage-b-results-next")) != 0 {
					t.Fatalf("crash must follow durable completion and precede the Wait reply: %+v", atCrash)
				}
			}
			if f.proxy.sends.Load() != int32(expectedCalls) || len(readRecoveryLog[ModelRequest](t, f.root, "models")) != expectedModels {
				t.Fatalf("unexpected dispatch before the %s crash", barrier)
			}

			resumed := f.recover(ctx, "resumed", "", false)
			resumed.wait()
			result := f.result("resumed")
			final := readRecoveryCheckpoint(t, f.root)
			models := readRecoveryLog[ModelRequest](t, f.root, "models")
			if result.Outcome != Succeeded || result.Cleanup != CleanupPending || len(models) != 2 || models[1].Step != 2 ||
				final.WriteEvidence["c1"] != f.proof.Ref || final.Balance.ToolRequests != 0 || final.Balance.ModelRequests != 0 ||
				final.Result == nil || !reflect.DeepEqual(result, *final.Result) || len(final.WriteEvidence) != 1 {
				t.Fatalf("wrong resumed outcome or final checkpoint: %+v, %+v", result, final)
			}
			tools := stageBToolResults(t, models[1].Messages)
			if len(tools) != 2 || tools[0].CallID != "c1" || tools[1].CallID != "c2" ||
				!reflect.DeepEqual(tools, stageBToolResults(t, result.Messages)) ||
				models[1].MaxOutputBytes != before.Balance.Output-jsonSize(t, tools[0])-jsonSize(t, tools[1]) {
				t.Fatalf("batch was replayed, reordered or recharged: %+v, budget %d", tools, models[1].MaxOutputBytes)
			}
			if barrier == "receipt" {
				if f.proxy.queries.Load() <= queries || final.Owner == nil || final.Owner.Epoch != 3 {
					t.Fatalf("pending result was not queried after the second takeover: %+v", final)
				}
			} else if f.proxy.queries.Load() != queries {
				t.Fatal("already settled c1 was queried again")
			}
			if barrier == "terminal" && (!reflect.DeepEqual(*atCrash.Result, result) || atCrash.Balance != final.Balance) {
				t.Fatal("terminal recovery changed its result or budget")
			}
			f.checkOriginalOnce(2)
			t.Logf("before=%s killed pid=%d resumed pid=%d queries=%d→%d c1 invokes=1 c2 invokes=1 next_model=1 output=%d→%d",
				barrier, next.pid, resumed.pid, queries, f.proxy.queries.Load(), before.Balance.Output, models[1].MaxOutputBytes)
		})
	}
}

func TestStageB3UnknownAfterTakeover(t *testing.T) {
	requireLocalRecovery(t)
	for _, scenario := range []struct {
		fault  string
		cancel bool
	}{
		{"missing-version", false}, {"wrong-proof", false}, {"query-down", false}, {"query-down", true},
	} {
		name := scenario.fault
		if scenario.cancel {
			name += "-cancel"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			f := newStageBRemoteFixture(t, false, true, 0, 3*time.Second)
			before := f.crashTool(ctx)
			f.old.stop()
			restore := f.blockStageBProof(scenario.fault)
			f.restartTool()
			next := f.recover(ctx, "next", "", scenario.cancel)
			admitted := readRecoveryCheckpoint(t, f.root)
			if admitted.Owner == nil || admitted.Owner.Epoch != 2 ||
				!admitted.ReconcileUntil.Equal(before.ReconcileUntil) || !admitted.Deadline.Equal(before.Deadline) {
				t.Fatalf("takeover reset the original deadline: before %+v after %+v", before, admitted)
			}
			if scenario.cancel {
				f.await(ctx, "stage-b-next-query")
				f.mark("stage-b-cancel", "yes")
			}
			next.wait()
			result := f.result("next")
			final := readRecoveryCheckpoint(t, f.root)
			trigger := StopReconcileDeadline
			if scenario.cancel {
				trigger = StopCancelled
			} else if final.FinishedAt.Before(before.ReconcileUntil) {
				t.Fatalf("Run finalized before original reconciliation cutoff: %+v", final)
			}
			if result.Outcome != ToolOutcomeUnknown || result.Stop == nil || result.Stop.CallID != "c1" || result.Stop.Trigger != trigger ||
				result.Cleanup != CleanupPending || !reflect.DeepEqual(result.Messages, before.Messages) ||
				len(readRecoveryLog[ModelRequest](t, f.root, "models")) != 1 || len(final.WriteEvidence) != 0 ||
				final.Balance.Output != before.Balance.Output-before.Intent.Limits.MaxResultBytes ||
				final.Balance.Scan != before.Balance.Scan-before.Intent.Limits.MaxScanBytes ||
				final.Balance.ToolRequests != before.Balance.ToolRequests {
				t.Fatalf("insufficient proof changed history, result, or reservation: %+v, %+v", result, final)
			}
			matched := false
			for _, request := range readRecoveryLog[remoteWireRequest](t, f.root, "remote-queries") {
				if request.Key != before.Intent.Key {
					t.Fatalf("changed original query key: %+v", request)
				}
				matched = matched || request.Owner.Epoch == 2
			}
			if !matched {
				t.Fatal("the new owner never attempted original reconciliation")
			}
			restore()
			proofContext := contract.WithWriteEvidenceQuery(ctx, f.proof.Intent)
			if proof, err := f.workspace.(contract.EvidenceWorkspace).ReadWriteEvidence(proofContext, f.proof.Intent); err != nil || proof.Ref != f.proof.Ref {
				t.Fatalf("late proof was not actually available: %+v (%v)", proof, err)
			}
			queries := f.proxy.queries.Load()
			replay := f.recover(ctx, "terminal-read", "", false)
			replay.wait()
			restored := readRecoveryCheckpoint(t, f.root)
			if !reflect.DeepEqual(result, f.result("terminal-read")) || !reflect.DeepEqual(restored, final) ||
				len(readRecoveryLog[ModelRequest](t, f.root, "models")) != 1 || f.proxy.queries.Load() != queries {
				t.Fatal("evidence restored after terminal changed the run or caused dispatch")
			}
			f.checkOriginalOnce(1)
			t.Logf("new pid=%d replay pid=%d original cutoff=%s c1 reserve=%d outcome=%s trigger=%s models_after_c1=0 resends=0",
				next.pid, replay.pid, before.ReconcileUntil.UTC().Format(time.RFC3339Nano), before.Intent.Limits.MaxResultBytes, result.Outcome, trigger)
		})
	}
}

func (f *stageBRemoteFixture) blockStageBProof(fault string) func() {
	t := f.t
	t.Helper()
	if fault == "query-down" {
		f.proxy.holdQueries.Store(true)
		return func() { f.proxy.holdQueries.Store(false) }
	}
	glob := "managed/*/*.blob"
	if fault == "wrong-proof" {
		glob = "managed/*/index.json"
	}
	path := evidenceFixturePath(t, f.root, glob)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if fault == "missing-version" {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	} else {
		var index struct {
			Paths  map[string]contract.FileVersion
			Writes map[string]contract.WriteEvidence
		}
		if err := json.Unmarshal(raw, &index); err != nil {
			t.Fatal(err)
		}
		bad := f.proof
		bad.Intent.AppendHash = contract.HashBytes([]byte("wrong content"))
		index.Writes[bad.Ref.ID] = bad
		if _, err := durable.Write(path, index); err != nil {
			t.Fatal(err)
		}
	}
	return func() {
		if _, err := durable.WriteBytes(path, raw); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStageB4RecoveredWriteHonorsContinuationGates(t *testing.T) {
	requireLocalRecovery(t)
	for _, scenario := range []struct {
		name, barrier string
		maxRequests   uint64
		outcome       RunOutcome
		code          StopCode
	}{
		{"workspace", "workspace", 2, Failed, StopWorkspaceUnavailable},
		{"budget", "first", 1, BudgetExceeded, StopModelRequestLimit},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 9*time.Second)
			defer cancel()
			f := newStageBRemoteFixture(t, false, false, scenario.maxRequests, 3*time.Second)
			before := f.crashTool(ctx)
			f.old.stop()
			f.restartTool()
			next := f.recover(ctx, "next", scenario.barrier, false)
			f.await(ctx, "stage-b-"+scenario.barrier)
			settled := readRecoveryCheckpoint(t, f.root)
			if len(settled.ToolResults) != 1 {
				t.Fatalf("gate ran before c1 was durably settled: %+v", settled)
			}
			stageBCheckSettled(t, before, settled, f.proof, settled.ToolResults[0])
			if scenario.name == "workspace" {
				path := filepath.Join(f.root, "workspace")
				if err := os.Rename(path, path+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			}
			f.mark("stage-b-release-"+scenario.barrier, "yes")
			next.wait()
			result := f.result("next")
			final := readRecoveryCheckpoint(t, f.root)
			if result.Outcome != scenario.outcome || result.Stop == nil || result.Stop.Code != scenario.code ||
				result.Cleanup != CleanupPending || final.Balance != settled.Balance || final.WriteEvidence["c1"] != f.proof.Ref ||
				len(readRecoveryLog[ModelRequest](t, f.root, "models")) != 1 {
				t.Fatalf("gate allowed continuation or changed c1 settlement: %+v, %+v", result, final)
			}
			f.checkOriginalOnce(1)
			t.Logf("new pid=%d stop=%s c1 settlement=1 model_after_gate=0 resends=0", next.pid, scenario.code)
		})
	}
}
