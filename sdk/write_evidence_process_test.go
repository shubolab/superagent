package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

const evidenceLine = "saved:c1\n"

type evidenceProcessWorkspace struct {
	contract.EnvironmentProvider
	contract.EvidenceWorkspace
	root, scenario string
	log            *recoveryFixture
}

func (w *evidenceProcessWorkspace) StageWrite(ctx context.Context, scope contract.CallScope, grant contract.EnvironmentGrantRef, intent contract.WriteIntent, data []byte) (contract.StagedWrite, error) {
	w.log.append("remote-executed", intent.Key)
	value, err := w.EvidenceWorkspace.StageWrite(ctx, scope, grant, intent, data)
	if err == nil && w.scenario == "evidence-after-stage" {
		err = w.stopAt(ctx, "evidence-staged")
	}
	if err == nil && w.scenario == "evidence-fenced-stage" {
		if err = writeTakeoverMarker(w.root, "evidence-staged", "yes"); err == nil {
			err = waitTakeoverMarker(ctx, w.root, "release-staged-write")
		}
		if err == nil {
			err = writeTakeoverMarker(w.root, "evidence-stage-released", "yes")
		}
	}
	return value, err
}

func (w *evidenceProcessWorkspace) CommitWrite(ctx context.Context, scope contract.CallScope, grant contract.EnvironmentGrantRef, intent contract.WriteIntent, staged contract.StagedWrite) (contract.WriteEvidence, error) {
	value, err := w.EvidenceWorkspace.CommitWrite(ctx, scope, grant, intent, staged)
	if err == nil {
		w.log.append("write-evidence-commits", value)
		switch w.scenario {
		case "evidence-after-commit":
			err = w.stopAt(ctx, "evidence-committed")
		case "before-journal", "disconnect-before-journal":
			err = w.stopAt(ctx, "remote-file-written")
		}
	}
	return value, err
}

func (w *evidenceProcessWorkspace) ReadWriteEvidence(ctx context.Context, intent contract.WriteIntent) (contract.WriteEvidence, error) {
	w.log.append("write-evidence-queries", intent.Key)
	if w.scenario == "evidence-cancel-ready" {
		if err := writeTakeoverMarker(w.root, "evidence-queried", "yes"); err != nil {
			return contract.WriteEvidence{}, err
		}
	}
	return w.EvidenceWorkspace.ReadWriteEvidence(ctx, intent)
}

func (w *evidenceProcessWorkspace) stopAt(ctx context.Context, point string) error {
	if err := writeTakeoverMarker(w.root, point, "yes"); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func remoteEvidenceWorkspace(t *testing.T, root string) (contract.EnvironmentProvider, RemoteToolIdentity) {
	t.Helper()
	env, err := NewLocalEvidenceEnvironment(map[contract.WorkspaceRef]WorkspaceConfig{"project": {
		Root: filepath.Join(root, "workspace"), ReadPrefixes: []string{"."},
	}}, EvidenceWorkspaceConfig{Directory: filepath.Join(root, "managed"), MaxFileBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	version, err := env.(contract.RecoveryWorkspace).RecoveryWorkspace(t.Context(), contract.RunScope{Workspace: "project"})
	if err != nil {
		t.Fatal(err)
	}
	return env, RemoteToolIdentity{Provider: "remote-tools", Journal: "original-journal", Version: "v1",
		Workspace: "project", WorkspaceVersion: version, EvidenceRule: AppendFileEvidenceV1}
}

type writeEvidenceModel struct{ *recoveryFixture }

func (f writeEvidenceModel) Generate(_ context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
	f.append("models", req)
	reply := &contract.ModelReply{Finish: contract.ModelFinishStop, Text: "done"}
	if req.Step == 1 {
		args, _ := json.Marshal(map[string]string{"Path": "notes.txt", "Content": evidenceLine})
		reply = &contract.ModelReply{Finish: contract.ModelFinishToolCalls, Text: "save", ToolCalls: []contract.ModelToolCall{{
			CallID: "c1", Tool: "write", Arguments: args,
		}}}
	}
	return &completionStream{reply}, nil
}

func readManagedNotes(t *testing.T, env contract.EnvironmentProvider) (string, error) {
	t.Helper()
	ctx := t.Context()
	scope := contract.RunScope{Runner: "inspect", Subject: "owner", RunID: "inspect", Workspace: "project", Deadline: time.Now().Add(time.Minute)}
	handle, err := env.Allocate(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Release(context.Background(), scope, handle)
	cs := contract.CallScope{Run: scope, Handle: handle, CallID: "read", Tool: "read", Path: "notes.txt",
		Limits: contract.ToolLimits{MaxScanBytes: 128, MaxResultBytes: 1024}}
	grant, err := env.Grant(ctx, cs, ".")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := env.OpenRead(ctx, cs, grant, cs.Path)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	return string(data), err
}

func evidenceFixturePath(t *testing.T, root, glob string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, glob))
	if err != nil || len(paths) != 1 {
		t.Fatalf("expected one fixture file %s, got %v (%v)", glob, paths, err)
	}
	return paths[0]
}

func TestWriteEvidenceRecoveryAfterRemoteProcessExit(t *testing.T) {
	requireLocalRecovery(t)
	for _, scenario := range []string{"committed", "staged-only", "unmanaged-matching-file", "missing-version", "wrong-proof", "missing-rule", "cancel-before-proof"} {
		t.Run(scenario, func(t *testing.T) {
			root := makeRecoveryRoot(t)
			makeRemoteCertificates(t, root)
			env, identity := remoteEvidenceWorkspace(t, root)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			cancelBeforeProof := scenario == "cancel-before-proof"
			staged := scenario == "staged-only" || scenario == "unmanaged-matching-file" || cancelBeforeProof
			barrier, provider := "evidence-committed", "evidence-after-commit"
			if staged {
				barrier, provider = "evidence-staged", "evidence-after-stage"
			}
			_, endpoint, kill := launchRemoteService(t, root, "original", provider)
			proxy := newRemoteProxy(t, root, endpoint, "evidence")
			f := writeEvidenceModel{defaultRecoveryFixture(root)}
			reconcileTimeout := 1350 * time.Millisecond
			if cancelBeforeProof {
				reconcileTimeout = 5 * time.Second
			}
			client := remoteRunClient(t, root, proxy.server.URL, identity, f, reconcileTimeout, false)
			run, err := client.Start(ctx, contract.StartRequest{ID: recoveryID, Model: "model", Input: contract.UserMessage{Text: "save once"},
				Recoverable: true, Tools: []contract.ToolRef{"write"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := waitTakeoverMarker(ctx, root, barrier); err != nil {
				t.Fatal(err)
			}
			journalPath := evidenceFixturePath(t, root, "receipts/*.receipt")
			beforeJournal, err := os.ReadFile(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			var pending struct {
				Key    contract.ReceiptKey
				Write  *contract.WriteIntent
				Result *contract.ToolResult
			}
			if err := json.Unmarshal(beforeJournal, &pending); err != nil || pending.Result != nil || pending.Write == nil || pending.Key.CallID != "c1" {
				t.Fatalf("wrong crash boundary: %s (%v)", beforeJournal, err)
			}
			before := readRecoveryCheckpoint(t, root)
			kill()
			var evidence contract.WriteEvidence
			var indexPath string
			var originalIndex []byte
			env, _ = remoteEvidenceWorkspace(t, root)
			if staged {
				if _, err := readManagedNotes(t, env); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("staged data became visible: %v", err)
				}
				if records, _ := filepath.Glob(filepath.Join(root, "managed", "*", "index.json")); len(records) != 0 {
					t.Fatalf("staging published an index: %v", records)
				}
				_ = evidenceFixturePath(t, root, "managed/*/*.blob")
			} else {
				indexPath = evidenceFixturePath(t, root, "managed/*/index.json")
				originalIndex, err = os.ReadFile(indexPath)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := readManagedNotes(t, env)
				if err != nil || raw != evidenceLine {
					t.Fatalf("published file after process exit: %q (%v)", raw, err)
				}
				workspace := env.(contract.EvidenceWorkspace)
				queryCtx, end := contract.WithWriteEvidenceQuery(ctx, *pending.Write)
				evidence, err = workspace.ReadWriteEvidence(queryCtx, *pending.Write)
				end()
				if err != nil || evidence.Intent.Key != pending.Key || evidence.Version.ID != contract.HashBytes([]byte(raw)) ||
					evidence.Version.Bytes != uint64(len(raw)) {
					t.Fatalf("published version and evidence disagree: %+v (%v)", evidence, err)
				}
			}
			switch scenario {
			case "unmanaged-matching-file":
				if err := os.WriteFile(filepath.Join(root, "workspace", "notes.txt"), []byte(evidenceLine), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-version":
				if err := os.Remove(evidenceFixturePath(t, root, "managed/*/*.blob")); err != nil {
					t.Fatal(err)
				}
			case "wrong-proof", "missing-rule":
				var index struct {
					Paths  map[string]contract.FileVersion
					Writes map[string]contract.WriteEvidence
				}
				if err := json.Unmarshal(originalIndex, &index); err != nil {
					t.Fatal(err)
				}
				changed := evidence
				if scenario == "wrong-proof" {
					changed.Intent.AppendHash = contract.HashBytes([]byte("another write"))
				} else {
					changed.Intent.Key.Remote.EvidenceRule = ""
				}
				index.Writes[evidence.Ref.ID] = changed
				if _, err := durable.Write(indexPath, index); err != nil {
					t.Fatal(err)
				}
			}
			replacementMode, trigger := "evidence-ready", contract.StopReconcileDeadline
			if cancelBeforeProof {
				replacementMode, trigger = "evidence-cancel-ready", contract.StopCancelled
			}
			_, replacement, _ := launchRemoteService(t, root, "replacement", replacementMode)
			proxy.target.Store(replacement)
			if cancelBeforeProof {
				if err := waitTakeoverMarker(ctx, root, "evidence-queried"); err != nil {
					t.Fatal(err)
				}
				if err := run.Cancel(ctx); err != nil {
					t.Fatal(err)
				}
			}
			result, err := run.Wait(ctx)
			if err != nil || result.Cleanup != contract.CleanupPending {
				t.Fatalf("Wait = %+v (%v)", result, err)
			}
			models := readRecoveryLog[contract.ModelRequest](t, root, "models")
			executed := readRecoveryLog[contract.ReceiptKey](t, root, "remote-executed")
			queries := readRecoveryLog[contract.ReceiptKey](t, root, "write-evidence-queries")
			if proxy.sends.Load() != 1 || len(executed) != 1 || pending.Key != executed[0] || len(queries) == 0 || queries[0] != pending.Key {
				t.Fatalf("original invocation or query identity changed: execution %v queries %v sends %d", executed, queries, proxy.sends.Load())
			}
			if scenario == "committed" {
				if result.Outcome != contract.Succeeded || len(models) != 2 || len(models[1].Messages) != 3 || len(result.Messages) != 4 {
					t.Fatalf("successful proof did not resume the original batch: %+v", result)
				}
				tool := models[1].Messages[2].Tool.Result
				encoded, _ := json.Marshal(tool)
				var output contract.AppendFileResult
				if err := json.Unmarshal([]byte(tool.Completion.Content), &output); err != nil || tool.CallID != "c1" ||
					output.Path != "notes.txt" || output.Version != evidence.Version.ID || output.Bytes != evidence.Version.Bytes ||
					models[1].MaxOutputBytes != before.Balance.Output-uint64(len(encoded)) || readRecoveryCheckpoint(t, root).Balance.ToolRequests != 0 {
					t.Fatalf("result or original budget was not settled once: %+v %v", tool, err)
				}
				if len(readRecoveryLog[contract.WriteEvidence](t, root, "write-evidence-commits")) != 1 {
					t.Fatal("query published another write")
				}
				final := evidenceFixturePath(t, root, "runs/*.run")
				var record struct {
					WriteEvidence map[contract.CallID]contract.WriteEvidenceRef
				}
				if raw, err := os.ReadFile(final); err != nil || json.Unmarshal(raw, &record) != nil {
					t.Fatalf("checkpoint evidence: %v", err)
				}
				if len(record.WriteEvidence) != 1 || record.WriteEvidence["c1"] != evidence.Ref {
					t.Fatalf("missing checkpoint provenance: %+v", record)
				}
				assertWriteEvidenceReadOnly(t, root, replacement, evidence, beforeJournal, originalIndex)
			} else if result.Outcome != contract.ToolOutcomeUnknown || result.Stop.CallID != "c1" || result.Stop.Trigger != trigger ||
				len(result.Messages) != 1 || len(models) != 1 || readRecoveryCheckpoint(t, root).Balance.Output != before.Balance.Output-before.Intent.Limits.MaxResultBytes {
				t.Fatalf("insufficient evidence advanced the original run: %+v", result)
			}
			if scenario == "wrong-proof" {
				if err := os.WriteFile(indexPath, originalIndex, 0600); err != nil {
					t.Fatal(err)
				}
				assertWriteEvidenceReadOnly(t, root, replacement, evidence, beforeJournal, originalIndex)
			}
			again, err := run.Wait(ctx)
			if err != nil || !reflect.DeepEqual(result, again) || len(readRecoveryLog[contract.ModelRequest](t, root, "models")) != len(models) {
				t.Fatalf("terminal changed after repeated wait: %+v (%v)", again, err)
			}
		})
	}
}

func assertWriteEvidenceReadOnly(t *testing.T, root, endpoint string, evidence contract.WriteEvidence, beforeJournal, originalIndex []byte) {
	t.Helper()
	send := readRecoveryLog[remoteWireRequest](t, root, "remote-sends")[0]
	query := remoteWireRequest{ID: "evidence-read", Key: send.Key, Scope: send.Scope, Limits: send.Limits}
	for i := 0; i < 2; i++ {
		status, reply := sendRemoteWire(t, root, endpoint, "reader", remoteReceiptPath, query)
		if status != http.StatusOK || reply.Receipt == nil || reply.Receipt.Evidence == nil || *reply.Receipt.Evidence != evidence.Ref {
			t.Fatalf("repeat proof query: %d %+v", status, reply)
		}
	}
	for glob, previous := range map[string][]byte{"receipts/*.receipt": beforeJournal, "managed/*/index.json": originalIndex} {
		now, err := os.ReadFile(evidenceFixturePath(t, root, glob))
		if err != nil || !bytes.Equal(now, previous) {
			t.Fatalf("read-only query mutated %s (%v)", glob, err)
		}
	}
}

func TestWriteEvidenceOldGenerationCannotPublishStagedData(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	makeRemoteCertificates(t, root)
	_, endpoint, _ := launchRemoteService(t, root, "provider", "evidence-fenced-stage")
	proxy := newRemoteProxy(t, root, endpoint, "evidence")
	killRunner, _ := launchRemoteRunner(t, root, proxy.server.URL, "original", "evidence-fenced-stage")
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := waitTakeoverMarker(ctx, root, "evidence-staged"); err != nil {
		t.Fatal(err)
	}
	before := readRecoveryCheckpoint(t, root)
	if before.Owner == nil || before.Owner.Epoch != 1 || before.Intent == nil {
		t.Fatalf("no original write admission: %+v", before)
	}
	killRunner()
	_, wait := launchRemoteRunner(t, root, proxy.server.URL, "recover", "evidence-fenced-stage")
	if err := waitTakeoverMarker(ctx, root, "remote-runner-accepted-recover"); err != nil {
		t.Fatal(err)
	}
	if err := writeTakeoverMarker(root, "release-staged-write", "yes"); err != nil {
		t.Fatal(err)
	}
	if err := waitTakeoverMarker(ctx, root, "evidence-stage-released"); err != nil {
		t.Fatal(err)
	}
	wait()
	after := readRecoveryCheckpoint(t, root)
	results := readRecoveryLog[contract.Result](t, root, "remote-runner-results")
	if after.Owner == nil || after.Owner.Epoch != 2 || len(results) != 1 || results[0].Outcome != contract.ToolOutcomeUnknown ||
		results[0].Stop.CallID != "c1" || proxy.sends.Load() != 1 ||
		len(readRecoveryLog[contract.WriteEvidence](t, root, "write-evidence-commits")) != 0 {
		t.Fatalf("expired writer changed the workspace or ran again: checkpoint %+v, results %+v", after, results)
	}
	env, _ := remoteEvidenceWorkspace(t, root)
	if _, err := readManagedNotes(t, env); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old epoch published after takeover: %v", err)
	}
}

func TestWriteEvidenceNativeReceiptTakesPriority(t *testing.T) {
	requireLocalRecovery(t)
	root := makeRecoveryRoot(t)
	makeRemoteCertificates(t, root)
	_, identity := remoteEvidenceWorkspace(t, root)
	_, endpoint, _ := launchRemoteService(t, root, "provider", "evidence-ready")
	req := contract.ToolRequest{RunID: "original", CallID: "c1", Tool: "write",
		Arguments: json.RawMessage(`{"path":"notes.txt","content":"saved:c1\n"}`),
		Limits:    contract.ToolLimits{MaxResultBytes: 1024, MaxScanBytes: 8}}
	key := contract.ReceiptKey{Remote: identity, Runner: "runner", Subject: "owner", RunID: req.RunID, Step: 1,
		CallID: req.CallID, Tool: req.Tool, BindingVersion: "v1", Fingerprint: durable.ToolFingerprint(req, "notes.txt")}
	scope := contract.RunScope{Runner: key.Runner, Subject: key.Subject, RunID: key.RunID, Deadline: time.Now().Add(8 * time.Second)}
	invoke := remoteWireRequest{ID: "write", Key: key, Scope: scope, Tool: &req, Limits: req.Limits}
	status, original := sendRemoteWire(t, root, endpoint, "owner", remoteInvokePath, invoke)
	if status != http.StatusOK || original.Receipt == nil || original.Receipt.Evidence != nil ||
		original.Receipt.Cleanup != contract.CleanupReleased {
		t.Fatalf("native reply: %d %+v", status, original)
	}
	// A readable, complete journal makes the write evidence unnecessary.
	if err := os.Remove(evidenceFixturePath(t, root, "managed/*/index.json")); err != nil {
		t.Fatal(err)
	}
	query := remoteWireRequest{ID: "read", Key: key, Scope: scope, Limits: req.Limits}
	status, reply := sendRemoteWire(t, root, endpoint, "reader", remoteReceiptPath, query)
	if status != http.StatusOK || !reflect.DeepEqual(original.Receipt, reply.Receipt) ||
		len(readRecoveryLog[contract.ReceiptKey](t, root, "write-evidence-queries")) != 0 {
		t.Fatalf("native result was not preferred: %d %+v", status, reply)
	}
}
