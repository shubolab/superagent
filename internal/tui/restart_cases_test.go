//go:build darwin || linux

package tui

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	sdk "github.com/shubolab/superagent"
	"github.com/shubolab/superagent/internal/durable"
)

func (p *restartProcess) ready() { p.expect("[甲 · 空闲]", "乙 · 空闲") }

func TestTUIRestartHistory(t *testing.T) {
	root := t.TempDir()
	old := startRestartProcess(t, root, "")
	old.ready()
	old.send("first-a\r")
	old.expect("[甲 · 已完成]", "助手 · answer-first-a")
	old.send("\tfirst-b\r")
	old.expect("[乙 · 已完成]", "助手 · answer-first-b")
	a1 := checkRestartIndex(t, root, "甲", 1, "").Receipts[0]
	b1 := checkRestartIndex(t, root, "乙", 1, "").Receipts[0]
	old.stop(false)

	reopened := startRestartProcess(t, root, "")
	onlyRestartHistory(t, reopened.expect("[甲 · 空闲]", "乙 · 空闲", "你 · first-a", "助手 · answer-first-a"), "first-b")
	reopened.send("\t")
	onlyRestartHistory(t, reopened.expect("[乙 · 空闲]", "你 · first-b", "助手 · answer-first-b"), "first-a")
	checkRestartCount(t, root, "model", "", 2)
	reopened.send("second-b\r")
	reopened.expect("[乙 · 已完成]", "助手 · answer-second-b")
	reopened.send("\tsecond-a\r")
	reopened.expect("[甲 · 已完成]", "助手 · answer-second-a")
	reopened.stop(false)

	for _, record := range []struct {
		label, side string
		prior       sdk.RunID
	}{{"甲", "a", a1.RunID}, {"乙", "b", b1.RunID}} {
		index := checkRestartIndex(t, root, record.label, 2, "")
		if index.Receipts[0].RunID != record.prior {
			t.Fatal("original receipt changed")
		}
		for _, receipt := range index.Receipts {
			checkRestartCount(t, root, "model", receipt.RunID, 1)
			checkRestartCount(t, root, "published", receipt.RunID, 1)
		}
		call := restartAudit(t, root, "model", index.Receipts[1].RunID)[0]
		want := []sdk.Message{
			{User: &sdk.UserMessage{Text: "first-" + record.side}},
			{Assistant: &sdk.AssistantMessage{Text: "answer-first-" + record.side}},
			{User: &sdk.UserMessage{Text: "second-" + record.side}},
		}
		if call.Model.Step != 1 || !reflect.DeepEqual(call.Model.Messages, want) {
			t.Fatalf("%s request history = %+v", record.label, call.Model.Messages)
		}
		t.Logf("%s history v2, prior=%s next=%s", record.label, record.prior, call.ID)
	}

	key, err := durable.Digest([2]string{"owner", "甲"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sessions", key+".session"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	broken := startRestartProcess(t, root, "")
	broken.expect("[甲 · 历史不可用]", "乙 · 空闲")
	broken.send("\tthird-b\r")
	broken.expect("[乙 · 已完成]", "助手 · answer-third-b")
	checkRestartIndex(t, root, "乙", 3, "")
	broken.stop(false)
	checkRestartCount(t, root, "cancel", "", 0)
}

func onlyRestartHistory(t *testing.T, frame, other string) {
	t.Helper()
	if strings.Contains(frame, other) {
		t.Fatalf("unexpected history or preview %q in %q", other, frame)
	}
}

func TestTUIRestartOriginalReceipt(t *testing.T) {
	root := t.TempDir()
	old := startRestartProcess(t, root, "receipt")
	old.ready()
	old.send("tool-a\r")
	barrier := awaitRestartEvent(t, root, "receipt", "")
	id := barrier.ID
	checkRestartIndex(t, root, "甲", 0, id)
	checkRestartCount(t, root, "tool", id, 1)
	original := restartAudit(t, root, "tool", id)[0]
	old.stop(true)

	reopened := startRestartProcess(t, root, "")
	frame := reopened.expect("[甲 · 可继续]", "乙 · 空闲", "RunID "+string(id), "Enter 继续原 Run")
	onlyRestartHistory(t, frame, "old-preview")
	checkRestartCount(t, root, "model", id, 1)
	checkRestartCount(t, root, "query", id, 0)
	checkRestartCount(t, root, "resume", id, 0)
	reopened.send("\tb-independent\r")
	reopened.expect("[乙 · 已完成]", "助手 · answer-b-independent")
	checkRestartIndex(t, root, "乙", 1, "")
	checkRestartIndex(t, root, "甲", 0, id)
	reopened.send("\t\r\r")
	frame = reopened.expect("[甲 · 已完成]", "助手 · answer-tool-a")
	onlyRestartHistory(t, frame, "old-preview")
	reopened.stop(false)

	tools := restartAudit(t, root, "query", id)
	models := restartAudit(t, root, "model", id)
	if len(tools) != 1 || *tools[0].Key != *original.Key || len(models) != 2 || models[0].Model.Step != 1 ||
		models[1].Model.Step != 2 || models[1].Model.MaxOutputBytes >= models[0].Model.MaxOutputBytes {
		t.Fatalf("reconciliation or remaining budget changed: queries=%+v requests=%+v", tools, models)
	}
	messages := models[1].Model.Messages
	if len(messages) != 3 || messages[2].Tool == nil || messages[2].Tool.Result.RunID != id ||
		messages[2].Tool.Result.Completion.Content != "original-effect" {
		t.Fatalf("missing reconciled original result: %+v", messages)
	}
	idx := checkRestartIndex(t, root, "甲", 1, "")
	if idx.Receipts[0].RunID != id {
		t.Fatal("resumption replaced RunID")
	}
	for _, kind := range []string{"start", "resume", "tool", "observe", "published"} {
		checkRestartCount(t, root, kind, id, 1)
	}
	checkRestartCount(t, root, "cancel", "", 0)
	t.Logf("甲 %s Step 1 → 2, actual tool=1 query=1 history=v1", id)
}

func TestTUIRestartTerminalSave(t *testing.T) {
	for _, source := range []string{"terminal", "sealed"} {
		t.Run(source, func(t *testing.T) {
			root := t.TempDir()
			old := startRestartProcess(t, root, source)
			old.ready()
			old.send("terminal-a\r")
			id := awaitRestartEvent(t, root, "before-publish", "").ID
			checkRestartIndex(t, root, "甲", 0, id)
			old.stop(true)
			mode := ""
			if source == "sealed" {
				expireRestartResult(t, root, id)
				mode = "save-failure"
			}
			reopened := startRestartProcess(t, root, mode)
			if source == "sealed" {
				reopened.expect("[甲 · 待保存]", "乙 · 空闲", "RunID "+string(id), "Enter 重试保存")
				checkRestartIndex(t, root, "甲", 0, id)
				reopened.send("\tindependent-b\r")
				reopened.expect("[乙 · 已完成]", "助手 · answer-independent-b", "甲 · 待保存")
				reopened.send("\t\r\r")
			}
			reopened.expect("[甲 · 已完成]", "助手 · answer-terminal-a")
			receipt := checkRestartIndex(t, root, "甲", 1, "").Receipts[0]
			if receipt.RunID != id {
				t.Fatal("supplement replaced original ID")
			}
			reopened.stop(false)

			again := startRestartProcess(t, root, "")
			frame := again.expect("[甲 · 空闲]", "乙 · 空闲", "助手 · answer-terminal-a")
			if strings.Count(frame, "answer-terminal-a") != 1 || checkRestartIndex(t, root, "甲", 1, "").Receipts[0] != receipt {
				t.Fatal("history was duplicated or replaced")
			}
			again.stop(false)
			checkRestartCount(t, root, "model", id, 1)
			checkRestartCount(t, root, "tool", id, 0)
			checkRestartCount(t, root, "query", id, 0)
			checkRestartCount(t, root, "resume", id, 0)
			checkRestartCount(t, root, "published", id, 1)
			checkRestartCount(t, root, "cancel", "", 0)
			t.Logf("甲 %s history=v1 after both reopens", id)
		})
	}
}

func TestTUIRestartPureQueue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		force bool
	}{{"Ctrl+D", false}, {"kill", true}} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			old := startRestartProcess(t, root, "hold")
			old.ready()
			old.send("hold-a\r")
			aID := awaitRestartEvent(t, root, "checkpoint", "").ID
			old.send("\tqueue-b\r")
			old.expect("[乙 · 排队中]")
			starts := restartAudit(t, root, "start", "")
			bID := starts[1].ID
			checkRestartIndex(t, root, "乙", 0, "")
			old.stop(tc.force)
			if !tc.force && !strings.Contains(old.screens.tail, "乙 · 未发送") {
				t.Fatalf("graceful exit did not confirm withdrawal: %q", old.screens.tail)
			}

			reopened := startRestartProcess(t, root, "")
			reopened.expect("[甲 · 可继续]", "乙 · 空闲", "RunID "+string(aID))
			reopened.send("\t")
			onlyRestartHistory(t, reopened.expect("[乙 · 空闲]", "> "), "queue-b")
			checkRestartIndex(t, root, "甲", 0, aID)
			checkRestartIndex(t, root, "乙", 0, "")
			checkRestartCount(t, root, "model", "", 0)
			reopened.send("queue-b\r")
			reopened.expect("[乙 · 已完成]", "助手 · answer-queue-b")
			saved := checkRestartIndex(t, root, "乙", 1, "").Receipts[0]
			if saved.RunID == bID {
				t.Fatal("new input reused the withdrawn RunID")
			}
			reopened.stop(false)
			checkRestartCount(t, root, "model", bID, 0)
			checkRestartCount(t, root, "model", saved.RunID, 1)
			checkRestartCount(t, root, "cancel", "", 0)
			t.Logf("甲 retained %s; 乙 old %s dispatched 0; new %s history=v1", aID, bID, saved.RunID)
		})
	}
}

func TestTUIRestartAdmissionBoundary(t *testing.T) {
	for _, mode := range []string{"reserved", "admitted", "attempted"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			old := startRestartProcess(t, root, mode)
			old.ready()
			old.send("seed-a\r")
			old.expect("[甲 · 已完成]")
			old.send("\tgrant-b\r")
			id := awaitRestartEvent(t, root, "grant-boundary", "").ID
			pending := checkRestartIndex(t, root, "乙", 0, id).Pending
			if pending.Attempted != (mode != "reserved") {
				t.Fatalf("wrong attempted flag %+v", pending)
			}
			f := restartFixture{root: root}
			_, err := os.Stat(f.checkpointPath(id))
			if (err == nil) != (mode == "admitted") {
				t.Fatalf("wrong checkpoint availability: %v", err)
			}
			old.stop(true)

			reopened := startRestartProcess(t, root, "")
			switch mode {
			case "reserved":
				reopened.expect("[甲 · 空闲]", "乙 · 空闲", "answer-seed-a")
				reopened.send("\t")
				reopened.expect("[乙 · 空闲]", "> grant-b")
				checkRestartIndex(t, root, "乙", 0, "")
			case "admitted":
				reopened.expect("乙 · 可继续")
				reopened.send("\t")
				reopened.expect("[乙 · 可继续]", "RunID "+string(id))
				checkRestartIndex(t, root, "乙", 0, id)
			case "attempted":
				reopened.expect("乙 · 待核对")
				reopened.send("\t\r")
				reopened.expect("[乙 · 待核对]", "RunID "+string(id))
				checkRestartIndex(t, root, "乙", 0, id)
			}
			reopened.stop(false)
			checkRestartCount(t, root, "model", id, 0)
			checkRestartCount(t, root, "resume", id, 0)
			checkRestartCount(t, root, "cancel", "", 0)
			t.Logf("乙 %s (%s) history=v0 model=0", id, mode)
		})
	}
}

func TestTUIRestartRecoveryGate(t *testing.T) {
	for _, mode := range []string{"live", "missing", "corrupt", "binding", "expired", "deadline", "stopped"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			seed := startRestartProcess(t, root, "")
			seed.ready()
			seed.send("prior-a\r")
			seed.expect("[甲 · 已完成]")
			prior := checkRestartIndex(t, root, "甲", 1, "").Receipts[0]
			seed.stop(false)
			source, input, barrier := "hold", "hold-a", "checkpoint"
			if mode == "expired" {
				source, input, barrier = "terminal", "terminal-a", "before-publish"
			}
			if mode == "deadline" {
				source = "deadline"
			}
			old := startRestartProcess(t, root, source)
			old.expect("[甲 · 空闲]", "乙 · 空闲", "助手 · answer-prior-a")
			old.send(input + "\r")
			id := awaitRestartEvent(t, root, barrier, "").ID
			checkRestartIndex(t, root, "甲", 1, id)
			if mode == "stopped" {
				old.expect("[甲 · 运行中]")
				old.send("\x1b")
				awaitRestartEvent(t, root, "cancel-accepted", id)
				checkpoint := readRestartCheckpoint(t, root, id)
				if checkpoint.Stop == nil || checkpoint.Stop.Code != sdk.StopCancelled || checkpoint.Result != nil {
					t.Fatalf("expected a retained stop before model dispatch: %+v", checkpoint)
				}
			}
			if mode != "live" {
				old.stop(true)
			}
			path := (restartFixture{root: root}).checkpointPath(id)
			switch mode {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "corrupt":
				if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
					t.Fatal(err)
				}
			case "expired":
				expireRestartResult(t, root, id)
			case "deadline":
				deadline := readRestartCheckpoint(t, root, id).Deadline
				if deadline.IsZero() {
					t.Fatal("missing original deadline")
				}
				if remaining := time.Until(deadline); remaining > 0 {
					time.Sleep(remaining)
				}
			}
			reopened := startRestartProcess(t, root, mode)
			status := "待核对"
			if mode == "live" || mode == "deadline" || mode == "stopped" {
				status = "可继续"
			}
			reopened.expect("[甲 · "+status+"]", "乙 · 空闲", "助手 · answer-prior-a", "RunID "+string(id))
			baseline := 0
			if mode == "expired" {
				baseline = 1
			}
			checkRestartCount(t, root, "model", id, baseline)
			reopened.send("\r")
			switch mode {
			case "live":
				reopened.expect("[甲 · 原进程占用]", "RunID "+string(id))
			case "deadline", "stopped":
				wantOutcome, wantCode, ui := sdk.TimedOut, sdk.StopDeadline, "运行超时"
				if mode == "stopped" {
					wantOutcome, wantCode, ui = sdk.Cancelled, sdk.StopCancelled, "已取消"
				}
				reopened.expect("[甲 · " + ui + "]")
				checkpoint := readRestartCheckpoint(t, root, id)
				if checkpoint.Result == nil || checkpoint.Result.Stop == nil || checkpoint.Result.Stop.Code != wantCode ||
					checkpoint.Result.Outcome != wantOutcome || checkRestartIndex(t, root, "甲", 2, "").Receipts[1].RunID != id {
					t.Fatalf("incorrect recovered terminal: %+v", checkpoint)
				}
			default:
				reopened.expect("[甲 · 待核对]", "RunID "+string(id))
			}
			reopened.send("\tgate-b\r")
			reopened.expect("[乙 · 已完成]", "助手 · answer-gate-b")
			checkRestartIndex(t, root, "乙", 1, "")
			expectedVersion, pending := uint64(1), id
			if mode == "deadline" || mode == "stopped" {
				expectedVersion, pending = 2, ""
			}
			if checkRestartIndex(t, root, "甲", expectedVersion, pending).Receipts[0] != prior {
				t.Fatal("original history changed")
			}
			reopened.stop(false)
			checkRestartCount(t, root, "model", id, baseline)
			checkRestartCount(t, root, "start", id, 1)
			if mode == "live" {
				old.stop(false)
			}
			if mode == "stopped" {
				checkRestartCount(t, root, "cancel", "", 1)
			} else {
				checkRestartCount(t, root, "cancel", "", 0)
			}
			t.Logf("甲 original %s (%s) gained 0 model dispatches", id, mode)
		})
	}
}

func expireRestartResult(t *testing.T, root string, id sdk.RunID) {
	t.Helper()
	path := (restartFixture{root: root}).checkpointPath(id)
	var record map[string]json.RawMessage
	if err := durable.Read(path, &record); err != nil {
		t.Fatal(err)
	}
	stamp, err := json.Marshal(time.Now().Add(-2 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	record["FinishedAt"] = stamp
	if _, err := durable.Write(path, record); err != nil {
		t.Fatal(err)
	}
}

func readRestartCheckpoint(t *testing.T, root string, id sdk.RunID) (out struct {
	Deadline time.Time
	Stop     *sdk.StopDetail
	Result   *sdk.Result
}) {
	t.Helper()
	path := (restartFixture{root: root}).checkpointPath(id)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestTUIRestartResumeQueue(t *testing.T) {
	root := t.TempDir()
	old := startRestartProcess(t, root, "two")
	old.ready()
	old.send("hold-a\r")
	aID := awaitRestartEvent(t, root, "checkpoint", "").ID
	old.send("\thold-b\r")
	var bID sdk.RunID
	for bID == "" {
		for _, start := range restartAudit(t, root, "start", "") {
			if start.Input == "hold-b" {
				bID = start.ID
				break
			}
		}
		select {
		case <-old.ctx.Done():
			t.Fatal(old.ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	awaitRestartEvent(t, root, "checkpoint", bID)
	old.stop(true)
	f := restartFixture{root: root}
	before, err := os.ReadFile(f.checkpointPath(bID))
	if err != nil {
		t.Fatal(err)
	}

	reopened := startRestartProcess(t, root, "hold")
	reopened.expect("[甲 · 可继续]", "乙 · 可继续", "RunID "+string(aID))
	reopened.send("\r")
	reopened.expect("[甲 · 恢复中]")
	reopened.send("\t\r")
	reopened.expect("[乙 · 恢复排队中]", "RunID "+string(bID))
	checkRestartCount(t, root, "model", "", 0)
	reopened.stop(false)
	after, err := os.ReadFile(f.checkpointPath(bID))
	if err != nil || string(after) != string(before) {
		t.Fatalf("queued resume changed original checkpoint: %v", err)
	}

	third := startRestartProcess(t, root, "")
	third.expect("[甲 · 可继续]", "乙 · 可继续")
	third.send("\t")
	third.expect("[乙 · 可继续]", "RunID "+string(bID))
	checkRestartIndex(t, root, "乙", 0, bID)
	checkRestartCount(t, root, "model", bID, 0)
	third.send("\r")
	third.expect("[乙 · 已完成]", "助手 · answer-hold-b")
	if checkRestartIndex(t, root, "乙", 1, "").Receipts[0].RunID != bID {
		t.Fatal("resumed B with a different RunID")
	}
	third.stop(false)
	checkRestartCount(t, root, "model", bID, 1)
	checkRestartCount(t, root, "start", bID, 1)
	checkRestartCount(t, root, "resume", bID, 2)
	checkRestartCount(t, root, "cancel", "", 0)
	t.Logf("乙 %s checkpoint unchanged in queue, Step 1 once after third process confirms, history=v1", bID)
}
