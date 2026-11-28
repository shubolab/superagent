//go:build darwin || linux

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sdk "github.com/shubolab/superagent"
	"github.com/shubolab/superagent/internal/durable"
)

func searchRecoverReceipt(t *testing.T, binary string) {
	for _, mode := range []string{"probe", "production"} {
		t.Run(mode, func(t *testing.T) {
			s := newSearchCase(t, binary)
			old, id, _, entry := s.holdOriginal("recover-a")
			old.expect("[甲 · 运行中]", "工具 · search", "搜索中")
			old.send("\t")
			rejectFrame(t, old.expect("[乙 · 空闲]"), "工具 · search")
			old.send("\t")
			old.expect("[甲 · 运行中]", "搜索中")
			before := s.checkpoint(id)
			old.stop(true)
			s.file("docs/health.txt", "status=changed\n")

			child := mode
			if child == "production" {
				child = ""
			}
			newApp := s.start(s.workspace, child)
			frame := newApp.expect("[甲 · 可继续]", "RunID "+string(id), "乙 · 空闲")
			rejectFrame(t, frame, "搜索中")
			s.wire.count("recover-a", 1)
			s.probeCount("query", id, 0)
			s.index("甲", 0, id)
			newApp.send("\tcontinue-b\r")
			s.wire.next("continue-b").answer("only-b")
			newApp.expect("[乙 · 已完成]", "only-b")
			s.index("乙", 1, "")
			s.wire.count("recover-a", 1)
			newApp.send("\t\r\r")
			next := s.wire.next("recover-a")
			report := next.report(t, "original")
			if len(report.Matches) != 1 || report.Matches[0].Preview != "status=ready" || report.ScannedBytes != 13 {
				t.Fatalf("recovery read live file instead of original result: %+v", report)
			}
			next.answer("continued-a")
			frame = newApp.expect("[甲 · 已完成]", "continued-a", "status=ready")
			rejectFrame(t, frame, "status=changed")
			rejectFrame(t, frame, "搜索中")
			if strings.Count(frame, "工具 · search") != 1 {
				t.Fatalf("original result repeated: %s", frame)
			}
			newApp.stop(false)
			after := s.checkpoint(id)
			verifyRun(t, after, id, sdk.Succeeded, 2)
			receiptBytes, err := json.Marshal(entry.Result)
			must(t, err)
			// The live environment meter is gone. The Run uses its original
			// scan reservation and the verified receipt size, once each.
			if before.Intent == nil || before.Intent.Key != entry.Key || after.Intent != nil ||
				!after.Deadline.Equal(before.Deadline) || after.Balance.Scan != before.Balance.Scan-before.Intent.Limits.MaxScanBytes ||
				after.Balance.Output != before.Balance.Output-uint64(len(receiptBytes)+len("continued-a")) ||
				after.Balance.ToolRequests != before.Balance.ToolRequests {
				t.Fatalf("original budget/output changed when resuming: before=%+v after=%+v", before.Balance, after.Balance)
			}
			if s.index("甲", 1, "").Receipts[0].RunID != id {
				t.Fatal("recovery committed a different Run")
			}
			s.probeCount("execute", id, 1)
			s.probeCount("invoke", id, 1)
			if mode == "probe" {
				s.probeCount("query", id, 1)
				if event := s.probes("query", id)[0]; event.Key != entry.Key {
					t.Fatalf("queried another call: %+v", event)
				}
			}
			s.wire.count("recover-a", 2)
			t.Logf("%s RunID=%s CallID=%s Step 1→2; receipt=%d scanned=13 scan reservation charged=%d history=v1",
				mode, id, entry.Key.CallID, len(receiptBytes), before.Intent.Limits.MaxScanBytes)
		})
	}
}

func (s *searchCase) holdOriginal(input string) (*cliProcess, sdk.RunID, string, journalEntry) {
	s.t.Helper()
	old := s.start(s.workspace, "hold-receipt")
	old.ready()
	old.send(input + "\r")
	s.wire.next(input).search("original", "docs/health.txt", "status")
	event := s.awaitProbe("receipt-ready")
	s.index("甲", 0, event.Run)
	file, entry := s.receipt("original")
	if entry.Result == nil || entry.Result.RunID != event.Run || entry.Key.RunID != event.Run {
		s.t.Fatalf("the original receipt was not durable: %+v", entry)
	}
	meter := s.probes("result", event.Run)
	if len(meter) != 1 || meter[0].Key != entry.Key || meter[0].Bytes != 13 || meter[0].Size > 16<<10 {
		s.t.Fatalf("unexpected source usage before restart: %+v", meter)
	}
	return old, event.Run, file, entry
}

func searchSettledAndTerminal(t *testing.T, binary string) {
	t.Run("settled", func(t *testing.T) {
		s := newSearchCase(t, binary)
		old := s.start(s.workspace, "hold-settled")
		old.ready()
		old.send("settled-a\r")
		s.wire.next("settled-a").search("settled", "docs/health.txt", "status")
		id := s.awaitProbe("settled").Run
		before := s.checkpoint(id)
		_, entry := s.receipt("settled")
		s.wire.count("settled-a", 1)
		s.index("甲", 0, id)
		if before.Step != 2 || before.ModelInFlight || before.Intent != nil || len(before.Messages) != 3 ||
			before.Balance.Scan != scanLimit-13 || before.Balance.ToolRequests != 1 {
			t.Fatalf("fault missed the committed batch checkpoint: %+v", before)
		}
		old.stop(true)
		app := s.start(s.workspace, "probe")
		app.expect("[甲 · 可继续]", "RunID "+string(id))
		s.wire.count("settled-a", 1)
		app.send("\r")
		next := s.wire.next("settled-a")
		next.report(t, "settled")
		next.answer("settled-answer")
		app.expect("[甲 · 已完成]", "status=ready")
		app.stop(false)
		after := s.checkpoint(id)
		verifyRun(t, after, id, sdk.Succeeded, 2)
		if after.Balance.Scan != before.Balance.Scan || after.Balance.ToolRequests != before.Balance.ToolRequests {
			t.Fatalf("charged the settled result again: %+v vs %+v", before.Balance, after.Balance)
		}
		s.probeCount("execute", id, 1)
		s.probeCount("invoke", id, 1)
		s.probeCount("query", id, 0)
		checkStableSearchHistory(s, id, "settled-answer", 2)
		t.Logf("RunID=%s CallID=%s already settled; query=0; repeated charge=0; history=v1", id, entry.Key.CallID)
	})
	t.Run("terminal", func(t *testing.T) {
		s := newSearchCase(t, binary)
		old := s.start(s.workspace, "")
		old.ready()
		old.send("terminal-a\r")
		s.wire.next("terminal-a").search("finished", "docs/health.txt", "status")
		next := s.wire.next("terminal-a")
		next.report(t, "finished")
		store := s.store("甲")
		lock, err := store.Lock(t.Context())
		must(t, err)
		defer lock.Close()
		idx, err := store.Load()
		must(t, err)
		id := idx.Pending.Request.ID
		next.answer("finished-answer")
		verifyRun(t, s.terminalCheckpoint(id), id, sdk.Succeeded, 2)
		old.expect("[甲 · 保存中]")
		idx, err = store.Load()
		must(t, err)
		if idx.Version != 0 {
			t.Fatal("terminal was already published")
		}
		old.stop(true)
		must(t, lock.Close())
		_, entry := s.receipt("finished")
		app := s.start(s.workspace, "probe")
		app.expect("[甲 · 已完成]", "status=ready", "finished-answer")
		app.stop(false)
		s.probeCount("query", id, 0)
		s.probeCount("invoke", id, 0)
		checkStableSearchHistory(s, id, "finished-answer", 2)
		t.Logf("RunID=%s CallID=%s terminal supplement; no additional model/tool; history=v1", id, entry.Key.CallID)
	})
}

func checkStableSearchHistory(s *searchCase, id sdk.RunID, answer string, wireCount int) {
	s.t.Helper()
	original := s.index("甲", 1, "").Receipts[0]
	if original.RunID != id {
		s.t.Fatalf("lost original committed RunID: %+v", original)
	}
	app := s.start(s.workspace, "")
	frame := app.expect("[甲 · 空闲]", answer, "第 1 行 · status=ready")
	if strings.Count(frame, "工具 · search") != 1 || s.index("甲", 1, "").Receipts[0] != original {
		s.t.Fatalf("reopening changed already saved result: %s", frame)
	}
	app.stop(false)
	s.wire.count("", wireCount)
}

func searchUntrustedReceipt(t *testing.T, binary string) {
	for _, mode := range []string{"missing", "corrupt", "wrong-call", "wrong-fingerprint"} {
		t.Run(mode, func(t *testing.T) {
			s := newSearchCase(t, binary)
			old, id, path, entry := s.holdOriginal("unknown-a")
			before := s.checkpoint(id)
			old.stop(true)
			switch mode {
			case "missing":
				must(t, os.Remove(path))
			case "corrupt":
				must(t, os.WriteFile(path, []byte("invalid json"), 0600))
			case "wrong-call":
				entry.Key.CallID = "different-call"
				_, err := durable.Write(path, entry)
				must(t, err)
			case "wrong-fingerprint":
				entry.Key.Fingerprint = strings.Repeat("0", 64)
				_, err := durable.Write(path, entry)
				must(t, err)
			}
			app := s.start(s.workspace, "probe")
			app.expect("[甲 · 可继续]", "RunID "+string(id), "乙 · 空闲")
			s.probeCount("query", id, 0)
			app.send("\r")
			app.expect("[甲 · 恢复中]")
			app.send("\tindependent-b\r")
			s.wire.next("independent-b").answer("b-while-reconciling")
			app.expect("[乙 · 已完成]", "b-while-reconciling")
			app.send("\t")
			callID := before.Intent.Key.CallID
			frame := app.expect("[甲 · 工具结果待核对]", "Run · 工具结果待核对 ["+string(callID)+"]")
			rejectFrame(t, frame, "工具 · search")
			rejectFrame(t, frame, "status=ready")
			after := s.checkpoint(id)
			verifyRun(t, after, id, sdk.ToolOutcomeUnknown, 1)
			if !after.Deadline.Equal(before.Deadline) || after.ReconcileUntil.IsZero() || after.ReconcileUntil.After(before.Deadline) ||
				after.Result.Stop.CallID != callID || after.Result.Stop.Trigger != sdk.StopReconcileDeadline ||
				len(after.Result.Messages) != 1 || time.Now().Before(after.ReconcileUntil) {
				t.Fatalf("did not honor the persisted reconciliation window: %+v", after)
			}
			for _, probe := range s.probes("query", id) {
				if probe.Key != before.Intent.Key {
					t.Fatalf("receipt lookup changed identity: %+v", probe)
				}
			}
			queries := len(s.probes("query", id))
			if queries < 1 {
				t.Fatal("missing actual durable receipt lookup")
			}
			s.probeCount("invoke", id, 1)
			s.probeCount("execute", id, 1)
			s.wire.count("unknown-a", 1)
			s.index("乙", 1, "")
			receipt := s.index("甲", 1, "").Receipts[0]
			app.stop(false)
			reopened := s.start(s.workspace, "")
			frame = reopened.expect("[甲 · 空闲]", "Run · 工具结果待核对")
			rejectFrame(t, frame, "status=ready")
			if s.index("甲", 1, "").Receipts[0] != receipt {
				t.Fatal("unknown terminal was recommitted")
			}
			reopened.stop(false)
			t.Logf("%s RunID=%s CallID=%s query=%d, repeat invoke=0, new model=0, history=v1", mode, id, callID, queries)
		})
	}
	t.Run("live-and-root-changed", func(t *testing.T) {
		s := newSearchCase(t, binary)
		old, id, _, _ := s.holdOriginal("auth-a")
		same := s.start(s.workspace, "probe")
		same.expect("[甲 · 可继续]", "RunID "+string(id))
		same.send("\r")
		same.expect("[甲 · 原进程占用]", "RunID "+string(id))
		same.stop(false)
		old.stop(true)

		// Replace the directory at the same path while keeping its predecessor
		// around to rule out inode reuse.
		must(t, os.Rename(s.workspace, filepath.Join(s.root, "previous-workspace")))
		must(t, os.MkdirAll(filepath.Join(s.workspace, "docs"), 0700))
		s.file("docs/health.txt", "status=other-root\n")
		changed := s.start(s.workspace, "probe")
		changed.expect("[甲 · 可继续]", "RunID "+string(id))
		changed.send("\r")
		changed.expect("[甲 · 待核对]", "RunID "+string(id))
		changed.send("\t")
		changed.expect("[乙 · 空闲]")
		changed.send("changed-b\r")
		s.wire.next("changed-b").answer("b-still-independent")
		changed.expect("[乙 · 已完成]", "b-still-independent")
		s.index("甲", 0, id)
		s.index("乙", 1, "")
		s.probeCount("query", id, 0)
		s.probeCount("invoke", id, 1)
		s.probeCount("execute", id, 1)
		s.wire.count("auth-a", 1)
		changed.stop(false)
	})
}
