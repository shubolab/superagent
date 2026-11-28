//go:build darwin || linux

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	sdk "github.com/shubolab/superagent"
)

func TestSearchAcceptance(t *testing.T) {
	binary := builtCLI(t)
	cases := []struct {
		name string
		run  func(*testing.T, string)
	}{
		{"1-command-workspace", searchCommandWorkspace},
		{"2-two-model-steps", searchTwoSteps},
		{"3-search-limits", searchLimits},
		{"4-run-budget", searchRunBudget},
		{"5-recover-receipt", searchRecoverReceipt},
		{"6-settled-and-terminal", searchSettledAndTerminal},
		{"7-untrusted-receipt", searchUntrustedReceipt},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, binary) })
	}
}

func searchCommandWorkspace(t *testing.T, binary string) {
	s := newSearchCase(t, binary)
	plain := s.start("", "")
	plain.ready()
	plain.send("legacy\r")
	first := s.wire.next("legacy")
	checkTools(t, first.request, false)
	first.answer("legacy-answer")
	plain.expect("[甲 · 已完成]", "legacy-answer")
	oldID := s.index("甲", 1, "").Receipts[0].RunID
	plain.send("legacy-in-flight\r")
	// End the original model stream before a complete reply, by terminating the
	// original process. Its text checkpoint and older history must still open.
	legacy := s.wire.next("legacy-in-flight").request
	checkTools(t, legacy, false)
	checkHistory(t, legacy, "legacy", "legacy-answer", "legacy-in-flight")
	idx, err := s.store("甲").Load()
	must(t, err)
	pending := idx.Pending.Request.ID
	plain.stop(true)

	search := s.start(s.workspace, "")
	search.expect("[甲 · 可继续]", "legacy-answer", "RunID "+string(pending), "乙 · 空闲")
	search.send("\r")
	search.expect("[甲 · 运行失败]", "legacy-answer")
	resolved := s.index("甲", 2, "")
	if resolved.Receipts[0].RunID != oldID || resolved.Receipts[1].RunID != pending || s.checkpoint(pending).Result.Stop.Code != sdk.StopModelInterrupted {
		t.Fatalf("text-mode checkpoints changed: %+v", resolved)
	}
	search.send("\tnew-ready\r")
	resp := s.wire.next("new-ready")
	checkTools(t, resp.request, true)
	resp.answer("search-ready")
	search.expect("[乙 · 已完成]")
	search.stop(false)

	alias := filepath.Join(s.root, "state-alias")
	must(t, os.Symlink(filepath.Join(s.workspace, "docs"), alias))
	aliasedState := filepath.Join(s.root, "aliased-state")
	must(t, os.Mkdir(aliasedState, 0700))
	must(t, os.Symlink(filepath.Join(s.workspace, "docs"), filepath.Join(aliasedState, "search-receipts")))
	for _, tc := range []struct{ work, state, want string }{
		{"", s.state, "-workspace"},
		{filepath.Join(s.root, "missing"), s.state, "-workspace"},
		{filepath.Join(s.workspace, "docs", "health.txt"), s.state, "-workspace"},
		{s.workspace, filepath.Join(s.workspace, "nested-state"), "不能重叠"},
		{s.workspace, filepath.Join(alias, "missing"), "不能重叠"},
		{s.workspace, aliasedState, "不能重叠"},
		{filepath.Join(s.state, "search-receipts"), s.state, "不能重叠"},
	} {
		proc := s.startArgs("", []string{"-model", "fixture", "-workspace", tc.work, "-state-dir", tc.state})
		if err := proc.exited(); err == nil || !strings.Contains(proc.tail, tc.want) {
			t.Fatalf("invalid CLI config %+v: %v: %q", tc, err, proc.tail)
		}
	}
	s.wire.count("", 3)

	// Use a fresh history so each second request contains exactly one error result.
	for _, path := range []string{"absolute", "traversal", "symlink", "fifo"} {
		t.Run(path, func(t *testing.T) {
			x := newSearchCase(t, binary)
			outside := filepath.Join(x.root, "outside.txt")
			must(t, os.WriteFile(outside, []byte("outside-secret"), 0600))
			must(t, os.Symlink(outside, filepath.Join(x.workspace, "docs", "escape.txt")))
			must(t, syscall.Mkfifo(filepath.Join(x.workspace, "docs", "pipe"), 0600))
			candidate := map[string]string{
				"absolute": outside, "traversal": "../outside.txt", "symlink": "docs/escape.txt", "fifo": "docs/pipe",
			}[path]
			app := x.start(x.workspace, "")
			app.ready()
			app.send("reject\r")
			x.wire.next("reject").search("bad", candidate, "outside")
			rejected := x.wire.next("reject")
			results := rejected.request.results()
			if len(results) != 1 || !results[0].IsError || results[0].ToolUseID != "bad" {
				t.Fatalf("expected a paired refusal: %+v", results)
			}
			rejected.answer("refusal-ack")
			frame := app.expect("[甲 · 已完成]", "未执行", "权限拒绝")
			rejectFrame(t, frame, "outside-secret")
			id := x.index("甲", 1, "").Receipts[0].RunID
			r := x.checkpoint(id)
			if r.Balance.Scan != scanLimit || len(r.Result.Messages) != 4 ||
				r.Result.Messages[2].Tool.Result.State != sdk.ToolExecutionNotExecuted {
				t.Fatalf("refusal consumed byte allowance or fabricated success: %+v", r)
			}
			app.stop(false)
			t.Logf("%s: v1, scan=0, RunID=%s", path, id)
		})
	}
}

func searchTwoSteps(t *testing.T, binary string) {
	s := newSearchCase(t, binary)
	app := s.start(s.workspace, "")
	app.ready()
	app.send("find-a\r")
	call := s.wire.next("find-a")
	checkTools(t, call.request, true)
	flushed, release := call.splitSearch("a1", "docs/health.txt", "status")
	defer release()
	select {
	case <-flushed:
	case <-time.After(4 * time.Second):
		t.Fatal("SSE prefix was not flushed")
	}
	before := app.expect("待完整回复", "[甲 · 运行中]")
	rejectFrame(t, before, "搜索中")
	entries, err := filepath.Glob(filepath.Join(s.state, "search-receipts", "*.receipt"))
	must(t, err)
	if len(entries) != 0 {
		t.Fatal("tool dispatched before ModelCompleted")
	}
	app.send("\t")
	app.expect("[乙 · 空闲]")
	release()
	next := s.wire.next("find-a")
	report := next.report(t, "a1")
	if report.Coverage != "complete" || report.ScannedBytes != 13 || len(report.Matches) != 1 || report.Matches[0].Preview != "status=ready" {
		t.Fatalf("unexpected real-file report: %+v", report)
	}
	app.send("\t")
	app.expect("[甲 · 运行中]", "工具 · search", "已结束 · 待确认")
	next.answer("answer-a")
	frame := app.expect("[甲 · 已完成]", "docs/health.txt", "完整 · 1 命中 · 13 字节", "第 1 行 · status=ready")
	if strings.Count(frame, "工具 · search") != 1 {
		t.Fatalf("duplicated tool entry: %s", frame)
	}
	app.send("\t")
	rejectFrame(t, app.expect("[乙 · 空闲]"), "工具 · search")
	s.index("乙", 0, "")
	id := s.index("甲", 1, "").Receipts[0].RunID
	_, original := s.receipt("a1")
	if original.Key.RunID != id || original.Result == nil || s.checkpoint(id).Balance.Scan != scanLimit-13 {
		t.Fatalf("missing exact durable settlement: %+v", original)
	}
	verifyRun(t, s.checkpoint(id), id, sdk.Succeeded, 2)
	app.stop(false)
	reopened := s.start(s.workspace, "")
	reopened.expect("[甲 · 空闲]", "完整 · 1 命中 · 13 字节", "第 1 行 · status=ready")
	reopened.stop(false)
	s.wire.count("find-a", 2)
	t.Logf("RunID=%s CallID=%s step=2 model=2 receipt=1 scan=13 history=v1", id, original.Key.CallID)
}

func searchLimits(t *testing.T, binary string) {
	for _, tc := range []struct {
		name    string
		size    int
		content func() string
		reason  string
		label   string
	}{
		{"complete-zero", 32, func() string { return strings.Repeat("nope\n", 6) }, "", "完整 · 0 命中"},
		{"large", 8 << 20, nil, "scan_limit", "部分覆盖 · 扫描上限"},
		{"larger", 64 << 20, nil, "scan_limit", "部分覆盖 · 扫描上限"},
		{"line", 32 << 10, func() string { return strings.Repeat("q", 32<<10) }, "line_limit", "部分覆盖 · 单行上限"},
		{"preview", 1024, func() string { return "find-" + strings.Repeat("x", 1018) + "\n" }, "preview_limit", "部分覆盖 · 预览上限"},
		{"dense", 0, func() string { return strings.Repeat("find-small\n", 64) }, "match_limit", "部分覆盖 · 命中数上限"},
		{"receipt", 0, func() string { return strings.Repeat("find-"+strings.Repeat("\x1b", 246)+"\n", 64) }, "result_limit", "部分覆盖 · 回执上限"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSearchCase(t, binary)
			if tc.content == nil {
				sparseShortLines(t, filepath.Join(s.workspace, "fixture"), tc.size)
			} else {
				s.file("fixture", tc.content())
			}
			file, err := os.Stat(filepath.Join(s.workspace, "fixture"))
			must(t, err)
			app := s.start(s.workspace, "")
			app.ready()
			app.send("limits\r")
			s.wire.next("limits").search("limit", "fixture", "find")
			step := s.wire.next("limits")
			result := step.report(t, "limit")
			wantCoverage := "complete"
			if tc.reason != "" {
				wantCoverage = "partial"
			}
			if result.Coverage != wantCoverage || result.Reason != tc.reason || result.ScannedBytes > scanLimit ||
				(tc.reason == "scan_limit" && result.ScannedBytes != scanLimit) {
				t.Fatalf("incorrect coverage or scan amount: %+v", result)
			}
			step.answer("limits-ok")
			frame := app.expect("[甲 · 已完成]", tc.label)
			if strings.Count(frame, " 行 · ") > 3 {
				t.Fatalf("display exceeded preview line limit: %s", frame)
			}
			// The frame intentionally includes fixed terminal controls. Check only
			// the content projection for controls supplied by file data.
			body, _, _ := strings.Cut(frame, "\x1b7")
			rejectFrame(t, body, "\x1b")
			if tc.reason != "" {
				rejectFrame(t, frame, "完整 · 0 命中")
			}
			id := s.index("甲", 1, "").Receipts[0].RunID
			_, stored := s.receipt("limit")
			if stored.Result == nil || stored.Key.RunID != id || stored.Result.Usage == nil || *stored.Result.Usage.ScannedBytes != result.ScannedBytes {
				t.Fatalf("receipt identity or actual scan differs: %+v", stored)
			}
			encoded, err := json.Marshal(stored.Result)
			must(t, err)
			if len(encoded) > 16<<10 {
				t.Fatalf("receipt exceeds its encoded cap: %d", len(encoded))
			}
			app.stop(false)
			t.Logf("file=%d scan=%d receipt=%d peak_RSS=%d RunID=%s CallID=%s history=v1",
				file.Size(), result.ScannedBytes, len(encoded), app.peakRSS(), id, stored.Key.CallID)
		})
	}
}

func sparseShortLines(t *testing.T, path string, size int) {
	t.Helper()
	f, err := os.Create(path)
	must(t, err)
	defer f.Close()
	// Supply actual small lines through the entire configured scan window.
	// The sparse tail makes the source much larger without changing that window.
	_, err = f.WriteString(strings.Repeat("no hit here\n", (scanLimit/12)+2))
	must(t, err)
	must(t, f.Truncate(int64(size)))
}

func searchRunBudget(t *testing.T, binary string) {
	s := newSearchCase(t, binary)
	sparseShortLines(t, filepath.Join(s.workspace, "large"), 8<<20)
	app := s.start(s.workspace, "")
	app.ready()
	app.send("budget-a\r")
	s.wire.next("budget-a").search("first", "large", "find")
	next := s.wire.next("budget-a")
	if report := next.report(t, "first"); report.ScannedBytes != scanLimit || report.Reason != "scan_limit" {
		t.Fatalf("search did not spend original scan balance: %+v", report)
	}
	app.send("\tb-can-run\r")
	b := s.wire.next("b-can-run")
	b.answer("b-independent")
	app.expect("[乙 · 已完成]", "b-independent")
	next.search("second", "docs/health.txt", "status")
	app.expect("甲 · 预算已用尽", "[乙 · 已完成]")
	app.send("\t")
	frame := app.expect("[甲 · 预算已用尽]", "Run · 预算已用尽 · 扫描预算", "部分覆盖 · 扫描上限")
	rejectFrame(t, frame, "second")
	id := s.index("甲", 1, "").Receipts[0].RunID
	s.index("乙", 1, "")
	r := s.checkpoint(id)
	verifyRun(t, r, id, sdk.BudgetExceeded, 2)
	if r.Result.Stop.Code != sdk.StopToolScanLimit || r.Balance.Scan != 0 ||
		len(r.Result.Messages) != 3 || r.Result.Messages[2].Tool == nil {
		t.Fatalf("incorrect budget closure: %+v", r)
	}
	files, err := filepath.Glob(filepath.Join(s.state, "search-receipts", "*.receipt"))
	must(t, err)
	if len(files) != 1 {
		t.Fatalf("search ran after exhausting scan budget: %v", files)
	}
	app.stop(false)
	reopened := s.start(s.workspace, "")
	reopened.expect("[甲 · 空闲]", "Run · 预算已用尽", "部分覆盖 · 扫描上限")
	reopened.stop(false)
	s.wire.count("budget-a", 2)
	s.wire.count("b-can-run", 1)
	t.Logf("甲 RunID=%s scan=%d search=1 blocked=1 history=v1; 乙 history=v1", id, scanLimit)
}
