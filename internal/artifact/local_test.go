package artifact

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
)

func TestStoreReservationAndArtifactExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s, err := NewLocal(Config{Directory: t.TempDir(), MaxArtifacts: 2, MaxStoredBytes: 8, MaxArtifactBytes: 8,
		MaxPageBytes: 3, Retention: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	call := contract.ArtifactCall{Owner: contract.ArtifactOwner{Runner: "default", Subject: "owner", RunID: "one"},
		Tool: "capture", CallID: "call", MaxBytes: 8}
	if n, err := s.Begin(ctx, call); n != 8 || err != nil {
		t.Fatalf("reserve = %d, %v", n, err)
	}
	next := call
	next.Owner.RunID = "two"
	if n, err := s.Begin(ctx, next); n != 0 || !errors.Is(err, contract.ErrArtifactCapacity) {
		t.Fatalf("store was overallocated: %d, %v", n, err)
	}
	w, err := s.OpenWriter(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("eight!!!")); err != nil {
		t.Fatal(err)
	}
	ref, err := w.Complete()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := s.Settle(ctx, call, []contract.ArtifactRef{ref}); n != 8 || err != nil {
		t.Fatalf("publish = %d, %v", n, err)
	}
	page, err := s.ReadArtifact(ctx, call.Owner, ref, 6, 100)
	if err != nil || !page.EOF || page.NextOffset != 8 || string(page.Content) != "!!" {
		t.Fatalf("unexpected EOF page %+v, %v", page, err)
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		released, err := s.Begin(ctx, next)
		if err == nil && released == 8 {
			break
		}
		if !errors.Is(err, contract.ErrArtifactCapacity) {
			t.Fatalf("failed to reclaim old item: %v", err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("artifact retention did not expire")
		}
	}
	if _, err := s.ReadArtifact(ctx, call.Owner, ref, 0, 1); !errors.Is(err, contract.ErrArtifactUnavailable) {
		t.Fatalf("expired artifact readable: %v", err)
	}
	if entries, err := os.ReadDir(s.path); err != nil || len(entries) > 0 {
		t.Fatalf("expired file was retained: %v, %v", entries, err)
	}
}

func TestStoreRefusesOversizedWriteBlock(t *testing.T) {
	s, err := NewLocal(Config{Directory: t.TempDir(), MaxArtifacts: 1, MaxStoredBytes: 4 << 20, MaxArtifactBytes: 2 << 20,
		MaxPageBytes: 3, Retention: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	scope := contract.ArtifactCall{Owner: contract.ArtifactOwner{Runner: "default", Subject: "owner", RunID: "one"},
		Tool: "capture", CallID: "call", MaxBytes: 1 << 20}
	if _, err := s.Begin(t.Context(), scope); err != nil {
		t.Fatal(err)
	}
	writer, err := s.OpenWriter(t.Context(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := writer.Write(bytes.Repeat([]byte("x"), contract.ArtifactMaxBlockBytes+1)); n != 0 || !errors.Is(err, contract.ErrArtifactOutputLimit) {
		t.Fatalf("large block write = %d, %v", n, err)
	}
	if _, err := writer.Complete(); err == nil {
		t.Fatal("oversized write produced a complete artifact")
	}
	if n, err := s.Settle(t.Context(), scope, nil); n != 0 || err != nil {
		t.Fatalf("oversized block was counted as stored output: %d, %v", n, err)
	}
}
