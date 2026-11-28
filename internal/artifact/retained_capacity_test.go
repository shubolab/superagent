package artifact

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

func quotaStore(directory string) Config {
	return Config{Directory: directory, MaxArtifactBytes: 1024, MaxStoredBytes: 1536,
		MaxArtifacts: 2, MaxPageBytes: 128, Retention: time.Minute}
}

func quotaCall(id contract.RunID) contract.ArtifactCall {
	return contract.ArtifactCall{Owner: contract.ArtifactOwner{Runner: "runner", Subject: "owner", RunID: id},
		CallID: "call", Tool: "write", MaxBytes: 2048}
}

func quotaContext(ctx context.Context, deadline time.Time, op contract.ArtifactOperation, call contract.ArtifactCall, ref contract.ArtifactRef) context.Context {
	admission := contract.ArtifactAdmission{Operation: op, Call: call, Ref: ref, Deadline: deadline}
	if op == contract.ArtifactOpenWriter {
		return contract.WithArtifactWriterAdmission(ctx, admission)
	}
	return contract.WithArtifactAdmission(ctx, admission)
}

func TestRetainedQuotaWriterProcess(t *testing.T) {
	root := os.Getenv("SUPERAGENT_RETAINED_QUOTA_DIR")
	if root == "" {
		return
	}
	store, err := NewRetained(quotaStore(root))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(1800 * time.Millisecond)
	call := quotaCall("writer")
	ctx := t.Context()
	limit, err := store.Begin(quotaContext(ctx, deadline, contract.ArtifactReserve, call, contract.ArtifactRef{}), call)
	if err != nil || limit != 1024 {
		t.Fatalf("writer reservation: %d (%v)", limit, err)
	}
	call.MaxBytes = limit
	writer, err := store.OpenWriter(quotaContext(ctx, deadline, contract.ArtifactOpenWriter, call, contract.ArtifactRef{}), call)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if n, err := writer.Write(bytes.Repeat([]byte{0x11}, 72)); err != nil || n != 72 {
		t.Fatalf("partial write: %d (%v)", n, err)
	}
	fmt.Println(deadline.Format(time.RFC3339Nano))
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	select {
	case <-t.Context().Done():
	case <-timer.C:
		t.Fatal("parent did not stop the writer")
	}
}

func TestRetainedQuotaAcrossProcesses(t *testing.T) {
	if !durable.Supported {
		t.Skip("requires local durable storage")
	}
	root := filepath.Join(t.TempDir(), "retained")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	childCtx, kill := context.WithCancel(ctx)
	defer kill()
	child := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestRetainedQuotaWriterProcess$")
	child.Env = append(os.Environ(), "SUPERAGENT_RETAINED_QUOTA_DIR="+root)
	child.Stderr = os.Stderr
	log, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	stopWriter := sync.OnceFunc(func() {
		kill()
		_ = child.Wait()
	})
	defer stopWriter()
	stream := bufio.NewScanner(log)
	if !stream.Scan() {
		t.Fatalf("writer did not pause: %v", stream.Err())
	}
	deadline, err := time.Parse(time.RFC3339Nano, stream.Text())
	if err != nil {
		t.Fatalf("parse child deadline %q: %v", stream.Text(), err)
	}

	store, err := NewRetained(quotaStore(root))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	kept := quotaCall("kept")
	keptDeadline := time.Now().Add(time.Minute)
	access := func(op contract.ArtifactOperation, call contract.ArtifactCall, ref contract.ArtifactRef) context.Context {
		return quotaContext(ctx, keptDeadline, op, call, ref)
	}
	limit, err := store.Begin(access(contract.ArtifactReserve, kept, contract.ArtifactRef{}), kept)
	if err != nil || limit != 512 {
		t.Fatalf("second process did not respect shared byte allowance: %d (%v)", limit, err)
	}
	kept.MaxBytes = limit
	w, err := store.OpenWriter(access(contract.ArtifactOpenWriter, kept, contract.ArtifactRef{}), kept)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	payload := bytes.Repeat([]byte{0x22}, 257)
	if n, err := w.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("writing retained fixture: %d (%v)", n, err)
	}
	ref, err := w.Complete()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if size, err := store.Settle(access(contract.ArtifactPublish, kept, ref), kept, []contract.ArtifactRef{ref}); err != nil || size != uint64(len(payload)) {
			t.Fatalf("publish or re-confirm: %d (%v)", size, err)
		}
	}
	third := quotaCall("third")
	checkCount := func() {
		t.Helper()
		if limit, err := store.Begin(access(contract.ArtifactReserve, third, contract.ArtifactRef{}), third); !errors.Is(err, contract.ErrArtifactCapacity) || limit != 0 {
			t.Fatalf("count quota or active writer protection failed: %d (%v)", limit, err)
		}
	}
	checkCount()
	expiry := time.NewTimer(time.Until(deadline) + 8*time.Millisecond)
	defer expiry.Stop()
	select {
	case <-expiry.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	checkCount()
	stopWriter()
	staging := filepath.Join(root, ".record-interrupted")
	if err := os.WriteFile(staging, []byte("partial metadata"), 0600); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewRetained(quotaStore(root))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.ArtifactStoreID() != store.ArtifactStoreID() {
		t.Fatal("same directory changed identity after writer termination")
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("atomic-write staging was not reclaimed: %v", err)
	}
	if limit, err := reopened.Begin(access(contract.ArtifactReserve, third, contract.ArtifactRef{}), third); err != nil || limit != 1024 {
		t.Fatalf("abandoned slot was not reclaimed or per-artifact cap failed: %d (%v)", limit, err)
	}
	pieces, err := filepath.Glob(filepath.Join(root, "*.body"))
	if err != nil || len(pieces) != 1 {
		t.Fatalf("abandoned staged body survived reaping: %v (%v)", pieces, err)
	}
	page, err := reopened.ReadArtifact(access(contract.ArtifactRead, contract.ArtifactCall{Owner: kept.Owner}, ref), kept.Owner, ref, 0, 125)
	if err != nil || !bytes.Equal(page.Content, payload[:125]) {
		t.Fatalf("valid published artifact was removed by reaping: %+v (%v)", page, err)
	}
	third.MaxBytes = 1024
	limited, err := reopened.OpenWriter(access(contract.ArtifactOpenWriter, third, contract.ArtifactRef{}), third)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	if n, err := limited.Write(make([]byte, contract.ArtifactMaxBlockBytes+1)); n != 0 || !errors.Is(err, contract.ErrArtifactOutputLimit) {
		t.Fatalf("oversized block: %d (%v)", n, err)
	}
	if ref, err := limited.Complete(); err == nil || ref.ID != "" {
		t.Fatalf("over-limit write was sealed: %+v (%v)", ref, err)
	}
	t.Log("cross-process reservations, writer pin, retained body, count cap and capacity reclamation verified")
}
