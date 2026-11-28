package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
	api "github.com/shubolab/superagent/session"
)

func TestJSONLRecoveryDiscardsUnpublishedTail(t *testing.T) {
	if !durable.Supported {
		t.Skip("process locking is unavailable")
	}
	ctx := t.Context()
	scope := Scope{Subject: "owner", Session: "discussion", Runner: "local"}
	cfg := Config{Directory: t.TempDir(), MaxTurns: 4, MaxHistoryBytes: 4096}
	store, err := Open(ctx, cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	backend := Backend{store}
	requestDigest := strings.Repeat("a", 64)
	id := store.NewRunID()
	pending := Pending{FirstVersion: 1, Binding: requestDigest, RequestDigest: requestDigest,
		Request: contract.StartRequest{ID: id, Recoverable: true, Input: contract.UserMessage{Text: "hello"}}}
	if err := backend.Reserve(ctx, pending); err != nil {
		t.Fatal(err)
	}
	idx, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	result := contract.Result{RunID: id, Outcome: contract.Succeeded, Messages: []contract.Message{
		{User: &pending.Request.Input}, {Assistant: &contract.AssistantMessage{Text: "world"}},
	}}
	turn, _, err := store.PrepareCommit(idx, id, &result)
	if err != nil {
		t.Fatal(err)
	}
	// Emulate a process loss after the JSONL write and before the index update.
	if _, err := store.appendHistory(idx, turn); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.ReadTurn(ctx, 1); !errors.Is(err, ErrVersion) {
		t.Fatalf("unpublished history was visible: %v", err)
	}
	reopened, err := Open(ctx, cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	backend = Backend{reopened}
	for range 2 {
		receipt, err := backend.Publish(ctx, id, turn.Digest)
		if err != nil || Receipt(receipt) != turn.Receipt {
			t.Fatalf("replay: %+v (%v)", receipt, err)
		}
	}
	data, err := os.ReadFile(store.historyPath())
	if err != nil || bytes.Count(data, []byte{'\n'}) != 1 {
		t.Fatalf("history must contain one complete row: %q (%v)", data, err)
	}
	var row Turn
	if err := json.Unmarshal(data, &row); err != nil || row.Receipt != turn.Receipt {
		t.Fatalf("invalid JSONL row: %+v (%v)", row, err)
	}
}

func TestLocalHistory(t *testing.T) {
	if !durable.Supported {
		t.Skip("process locking is unavailable")
	}
	ctx := t.Context()
	scope := Scope{Subject: "owner", Session: "discussion", Runner: "local"}
	latest := []contract.Message{{User: &contract.UserMessage{Text: "second"}}, {Assistant: &contract.AssistantMessage{Text: "reply"}}}
	encoded, err := json.Marshal(latest)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Directory: t.TempDir(), MaxTurns: 2, MaxHistoryBytes: uint64(len(encoded))}
	store, err := Open(ctx, cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := store.Load()
	if err != nil || idx.Version != 0 || idx.Pending != nil {
		t.Fatalf("empty history: %+v, %v", idx, err)
	}
	backend := Backend{store}
	for version, messages := range [][]contract.Message{
		{{User: &contract.UserMessage{Text: strings.Repeat("a", 50)}}, {Assistant: &contract.AssistantMessage{Text: "first reply"}}},
		latest,
	} {
		digest, err := resultDigest(contract.Succeeded, messages)
		if err != nil {
			t.Fatal(err)
		}
		id := store.NewRunID()
		pending := Pending{Version: uint64(version), FirstVersion: 1, Binding: digest, RequestDigest: digest,
			Request: contract.StartRequest{ID: id, Recoverable: true, Input: *messages[0].User}}
		if err := backend.Reserve(ctx, pending); err != nil {
			t.Fatal(err)
		}
		receipt := api.Receipt{RunID: id, Version: uint64(version + 1), Digest: digest}
		if err := backend.Seal(ctx, api.Turn{Receipt: receipt, RequestDigest: digest, Outcome: contract.Succeeded, Messages: messages}); err != nil {
			t.Fatal(err)
		}
		if got, err := backend.Publish(ctx, id, digest); err != nil || got != receipt {
			t.Fatalf("publish: %+v (%v)", got, err)
		}
	}
	reopened, err := Open(ctx, cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	idx, err = reopened.Load()
	if err != nil {
		t.Fatal(err)
	}
	first, window, err := reopened.Window(idx)
	if err != nil || first != 2 || !reflect.DeepEqual(window, latest) {
		t.Fatalf("complete suffix: %d, %+v, %v", first, window, err)
	}
	if _, err := reopened.ReadTurn(idx, 1); err != nil {
		t.Fatal(err)
	}
	if err := reopened.CheckCapacity(idx, Pending{}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	smaller := cfg
	smaller.MaxHistoryBytes--
	limited, err := Open(ctx, smaller, scope)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := limited.Window(idx); !errors.Is(err, ErrHistoryLimit) {
		t.Fatalf("window limit: %v", err)
	}
	other := scope
	other.Subject = "other"
	separate, err := Open(ctx, cfg, other)
	if err != nil {
		t.Fatal(err)
	}
	empty, err := separate.Load()
	if err != nil || empty.Version != 0 || separate.Owns(idx.Receipts[0].RunID) {
		t.Fatalf("other subject: %+v, %v", empty, err)
	}
	other = scope
	other.Runner = "different"
	if _, err := Open(ctx, cfg, other); !errors.Is(err, ErrOwnership) {
		t.Fatalf("runner mismatch: %v", err)
	}
	history, err := os.OpenFile(store.historyPath(), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := history.WriteAt([]byte("!"), idx.HistoryEnds[0])
	if err := errors.Join(writeErr, history.Close()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Window(idx); !errors.Is(err, ErrStorage) {
		t.Fatalf("corrupt turn: %v", err)
	}
	if err := os.WriteFile(store.indexPath, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, cfg, scope); !errors.Is(err, ErrStorage) {
		t.Fatalf("corrupt index: %v", err)
	}
}

func TestOpenRejectsUnsupportedIndex(t *testing.T) {
	if !durable.Supported {
		t.Skip("process locking is unavailable")
	}
	ctx := t.Context()
	scope := Scope{Subject: "owner", Session: "discussion", Runner: "local"}
	cfg := Config{Directory: t.TempDir(), MaxTurns: 4, MaxHistoryBytes: 4096}
	store, err := Open(ctx, cfg, scope)
	if err != nil {
		t.Fatal(err)
	}
	old, err := json.Marshal(Index{Format: 1, Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(store.indexPath, old, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, cfg, scope); !errors.Is(err, ErrStorage) {
		t.Fatalf("unsupported index was accepted: %v", err)
	}
	data, err := os.ReadFile(store.indexPath)
	if err != nil || !bytes.Equal(data, old) {
		t.Fatalf("unsupported index was changed: %q (%v)", data, err)
	}
}

func TestAdmissionLockUsesOneFilePerSession(t *testing.T) {
	if !durable.Supported {
		t.Skip("process locking is unavailable")
	}
	ctx := t.Context()
	cfg := Config{Directory: t.TempDir(), MaxTurns: 2, MaxHistoryBytes: 128}
	store, err := Open(ctx, cfg, Scope{Subject: "owner", Session: "discussion", Runner: "local"})
	if err != nil {
		t.Fatal(err)
	}
	id := store.NewRunID()
	admission, err := store.AdmissionLock(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Close()
	idx, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	idx.Pending = &Pending{
		FirstVersion: 1, Binding: digest, RequestDigest: digest,
		Request: contract.StartRequest{ID: id, Recoverable: true},
	}
	if err := store.Save(idx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := store.AdmissionLock(ctx, store.NewRunID()); !errors.Is(err, ErrBusy) {
			t.Fatalf("another Run during admission: %v", err)
		}
	}
	files, err := filepath.Glob(filepath.Join(cfg.Directory, "*.admission.lock"))
	if err != nil || len(files) != 1 {
		t.Fatalf("admission lock files: %v, %v", files, err)
	}
}
