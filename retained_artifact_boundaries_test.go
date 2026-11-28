package superagent

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestRetainedArtifactUnavailableBoundaries(t *testing.T) {
	requireLocalRecovery(t)
	for _, fault := range []string{"missing", "corrupt", "cancel", "settled"} {
		t.Run(fault, func(t *testing.T) {
			cfg := retainedProcessConfig{Root: makeRecoveryRoot(t), Barrier: "receipt"}
			if fault == "settled" {
				cfg.Barrier = "batch"
			}
			if fault == "cancel" {
				cfg.Window = 3 * time.Second
			}
			writer := launchRetainedProcess(t, cfg, "start")
			waitRetainedPhase(t, cfg.Root, cfg.Barrier)
			before := readRecoveryCheckpoint(t, cfg.Root)
			ref := retainedOriginalRef(t, cfg.Root)
			writer.stop()

			if fault == "corrupt" {
				damageArtifactBlock(t, cfg.Root)
			} else if err := os.Remove(evidenceFixturePath(t, cfg.Root, "artifacts/*.body")); err != nil {
				t.Fatal(err)
			}

			cfg.Barrier = ""
			if fault == "cancel" {
				cfg.Barrier = "cancel"
			}
			resume := launchRetainedProcess(t, cfg, "resume")
			if fault == "cancel" {
				waitRetainedPhase(t, cfg.Root, "query")
				writeRetainedMarker(t, cfg.Root, "cancel")
			}
			resume.wait()
			result := retainedResult(t, cfg.Root, "resume")
			if fault == "settled" {
				cp := readRecoveryCheckpoint(t, cfg.Root)
				models := readRecoveryLog[ModelRequest](t, cfg.Root, "models")
				tools := stageBToolResults(t, result.Messages)
				if before.Step != 2 || before.Intent != nil || len(tools) != 1 ||
					!reflect.DeepEqual(result.Messages, before.Messages) || cp.Balance != before.Balance ||
					result.Outcome != Failed || result.Stop == nil || result.Stop.Code != StopArtifactUnavailable || len(models) != 1 {
					t.Fatalf("settled result changed or allowed dispatch: %+v at %+v", result, cp)
				}
				if len(readRecoveryLog[struct{}](t, cfg.Root, "artifact-invocations")) != 1 {
					t.Fatal("settled tool executed again")
				}
			} else {
				trigger := StopReconcileDeadline
				if fault == "cancel" {
					trigger = StopCancelled
				}
				checkRetainedUnknown(t, cfg.Root, before, result, trigger)
				validateOneArtifactExecution(t, cfg.Root, before)
			}
			cfg.Barrier = ""
			client := retainedProcessClient(t, t.Context(), cfg)
			assertArtifactUnavailable(t, t.Context(), client.Run(recoveryID), ref, 2*ArtifactMaxBlockBytes, 1)
			t.Logf("fault=%s recovery_pid=%d outcome=%s stop=%s, new model dispatches=0 resends=0",
				fault, resume.pid, result.Outcome, result.Stop.Code)
		})
	}
}

func TestRetainedArtifactTerminalReadAndExpiry(t *testing.T) {
	requireLocalRecovery(t)
	cfg := retainedProcessConfig{Root: makeRecoveryRoot(t), Timeout: 2700 * time.Millisecond, Retention: 40 * time.Millisecond}
	writer := launchRetainedProcess(t, cfg, "start")
	writer.wait()
	first := retainedResult(t, cfg.Root, "start")
	ref := retainedOriginalRef(t, cfg.Root)
	before := readRecoveryCheckpoint(t, cfg.Root)
	if first.Outcome != Succeeded || !ref.ExpiresAt.Equal(before.Deadline) {
		t.Fatalf("terminal outcome or retained deadline: %+v %+v", first, ref)
	}
	client := retainedProcessClient(t, t.Context(), cfg)
	handle := client.Run(recoveryID)
	verifyRetainedPages(t, handle, ref, cfg.storeConfig().MaxPageBytes)
	other := context.WithValue(t.Context(), retainedTestCallerKey{}, SubjectID("other"))
	assertArtifactUnavailable(t, other, handle, ref, 0, 1)
	assertArtifactUnavailable(t, t.Context(), client.Run("another-run"), ref, 0, 1)
	assertArtifactUnavailable(t, t.Context(), handle, ref, ref.SizeBytes+1, 1)
	assertArtifactUnavailable(t, t.Context(), handle, ref, 0, 0)
	for _, change := range []func(*ArtifactRef){
		func(x *ArtifactRef) { x.Version += "wrong" },
		func(x *ArtifactRef) { x.SizeBytes-- },
		func(x *ArtifactRef) { x.SHA256 = ref.Version },
		func(x *ArtifactRef) { x.ExpiresAt = x.ExpiresAt.Add(time.Minute) },
	} {
		tampered := ref
		change(&tampered)
		assertArtifactUnavailable(t, t.Context(), handle, tampered, 0, 1)
	}
	damageArtifactBlock(t, cfg.Root)
	assertArtifactUnavailable(t, t.Context(), handle, ref, 2*ArtifactMaxBlockBytes-5, 10)
	page, err := handle.ReadArtifact(t.Context(), ref, 0, 10)
	if err != nil || !bytes.Equal(page.Content, diskArtifactPayload[:10]) {
		t.Fatalf("independent verified page: %+v (%v)", page, err)
	}

	// Restore the original bytes so expiry is independently tested.
	path := evidenceFixturePath(t, cfg.Root, "artifacts/*.body")
	if err := os.WriteFile(path, diskArtifactPayload, 0600); err != nil {
		t.Fatal(err)
	}
	wait := time.NewTimer(time.Until(ref.ExpiresAt) + 5*time.Millisecond)
	defer wait.Stop()
	select {
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	case <-wait.C:
	}
	assertArtifactUnavailable(t, t.Context(), handle, ref, 0, 1)
	expired := launchRetainedProcess(t, cfg, "expired")
	expired.wait()
	if end := retainedResult(t, cfg.Root, "expired"); !reflect.DeepEqual(end, first) ||
		len(readRecoveryLog[struct{}](t, cfg.Root, "artifact-invocations")) != 1 ||
		len(readRecoveryLog[ModelRequest](t, cfg.Root, "models")) != 2 {
		t.Fatalf("expired artifact changed the original Run: %+v", end)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired binding did not reclaim the body: %v", err)
	}
	t.Logf("writer_pid=%d terminal_reader_pid=%d original expiry=%s, terminal retained and artifact reclaimed",
		writer.pid, expired.pid, ref.ExpiresAt.Format(time.RFC3339Nano))
}

func TestRetainedArtifactRejectsReplacementBinding(t *testing.T) {
	requireLocalRecovery(t)
	cfg := retainedProcessConfig{Root: makeRecoveryRoot(t), Barrier: "published"}
	writer := launchRetainedProcess(t, cfg, "start")
	waitRetainedPhase(t, cfg.Root, cfg.Barrier)
	ref := retainedOriginalRef(t, cfg.Root)
	before := readRecoveryCheckpoint(t, cfg.Root)
	writer.stop()
	original := cfg.storeConfig().Directory
	if err := os.Rename(original, original+"-original"); err != nil {
		t.Fatal(err)
	}
	cfg.Barrier = ""
	client := retainedProcessClient(t, t.Context(), cfg)
	if _, err := client.Recover(t.Context(), recoveryID); !errors.Is(err, ErrRecoveryBinding) {
		t.Fatalf("replacement accepted old Run: %v", err)
	}
	assertArtifactUnavailable(t, t.Context(), client.Run(recoveryID), ref, 0, 1)
	if cp := readRecoveryCheckpoint(t, cfg.Root); !reflect.DeepEqual(cp, before) || len(readRecoveryLog[ModelRequest](t, cfg.Root, "models")) != 1 {
		t.Fatalf("replacement advanced the original checkpoint: %+v", cp)
	}
	validateOneArtifactExecution(t, cfg.Root, before)
}

func TestRetainedArtifactLiveTakeover(t *testing.T) {
	requireLocalRecovery(t)
	for _, point := range []string{"old-publish", "before-seal"} {
		t.Run(point, func(t *testing.T) {
			cfg := retainedProcessConfig{Root: makeRecoveryRoot(t), Barrier: point, Old: true, Takeover: true}
			old := launchRetainedProcess(t, cfg, "start")
			waitRetainedPhase(t, cfg.Root, point)
			before := readRecoveryCheckpoint(t, cfg.Root)
			if err := writeTakeoverMarker(cfg.Root, retainedArtifactPhase+"freeze", strconv.FormatInt(time.Now().UnixNano(), 10)); err != nil {
				t.Fatal(err)
			}
			cfg.Barrier, cfg.Old = "", false
			next := launchRetainedProcess(t, cfg, "resume")
			waitRetainedPhase(t, cfg.Root, "accepted-resume")
			owned := readRecoveryCheckpoint(t, cfg.Root)
			if before.Owner == nil || owned.Owner == nil || before.Owner.Epoch != 1 || owned.Owner.Epoch != 2 ||
				owned.Owner.Instance == before.Owner.Instance {
				t.Fatalf("did not take over original invocation: %+v -> %+v", before.Owner, owned.Owner)
			}
			writeRetainedMarker(t, cfg.Root, "release-"+point)
			log := "artifact-old-publish-denied"
			if point == "before-seal" {
				log = "artifact-old-seal-denied"
			}
			ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
			defer cancel()
			if err := waitTakeoverMarker(ctx, cfg.Root, log); err != nil {
				t.Fatal(err)
			}
			writeRetainedMarker(t, cfg.Root, "finish-old")
			old.wait()
			if denied := readRecoveryLog[bool](t, cfg.Root, log); !reflect.DeepEqual(denied, []bool{true}) {
				t.Fatalf("the live old process was not fenced: %v", denied)
			}
			next.wait()
			result := retainedResult(t, cfg.Root, "resume")
			if point == "old-publish" {
				ref := retainedOriginalRef(t, cfg.Root)
				checkRetainedSettlement(t, cfg.Root, before, result, ref)
			} else {
				checkRetainedUnknown(t, cfg.Root, before, result, StopReconcileDeadline)
				if refs := readRecoveryLog[ArtifactRef](t, cfg.Root, "artifact-original-ref"); len(refs) != 0 {
					t.Fatalf("old process sealed a reference: %+v", refs)
				}
			}
			validateOneArtifactExecution(t, cfg.Root, before)
			t.Logf("phase=%s pid=%d→%d epoch=1→2 old denied, final outcome=%s", point, old.pid, next.pid, result.Outcome)
		})
	}
}

func writeRetainedMarker(t *testing.T, root, phase string) {
	t.Helper()
	if err := writeTakeoverMarker(root, retainedArtifactPhase+phase, "yes"); err != nil {
		t.Fatal(err)
	}
}

func damageArtifactBlock(t *testing.T, root string) {
	t.Helper()
	f, err := os.OpenFile(evidenceFixturePath(t, root, "artifacts/*.body"), os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	offset := 2 * ArtifactMaxBlockBytes
	_, err = f.WriteAt([]byte{diskArtifactPayload[offset] ^ 0xff}, int64(offset))
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		t.Fatal(err)
	}
}
