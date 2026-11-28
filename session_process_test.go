package superagent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func sessionLog(t *testing.T, root, file string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err == nil {
		err = appendRecoveryFile(filepath.Join(root, file), string(data)+"\n")
	}
	if err != nil {
		t.Fatal(err)
	}
}

func plainSessionClient(t *testing.T, root, binding string, retention time.Duration) *Client {
	t.Helper()
	return sessionTestClient(t, root, binding, retention, deliveryModel(func(_ context.Context, req ModelRequest) (ModelStream, error) {
		data, err := json.Marshal(req)
		if err == nil {
			err = appendRecoveryFile(filepath.Join(root, "session-models"), string(data)+"\n")
		}
		if err != nil {
			return nil, err
		}
		input := req.Messages[len(req.Messages)-1].User.Text
		return &completionStream{&ModelReply{Finish: ModelFinishStop, Text: "reply " + input}}, nil
	}), nil)
}

func sessionProcessCommand(ctx context.Context, root, action string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStageCSessionChild$")
	cmd.Env = append(os.Environ(), "SUPERAGENT_SESSION_TEST_DIR="+root, "SUPERAGENT_SESSION_TEST_ACTION="+action)
	return cmd
}

func runSessionChild(t *testing.T, root, action, point string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := sessionProcessCommand(ctx, root, action)
	if point == "" {
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("session process %s: %v\n%s", action, err, out)
		}
		return
	}
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(out)
	for scanner.Scan() {
		if scanner.Text() == "READY "+point {
			if err := cmd.Process.Kill(); err != nil {
				_ = cmd.Wait()
				t.Fatal(err)
			}
			if err := cmd.Wait(); err == nil {
				t.Fatalf("%s exited normally at %s", action, point)
			}
			return
		}
		t.Log(scanner.Text())
	}
	err = cmd.Wait()
	t.Fatalf("%s missed process barrier %s: %v", action, point, err)
}

func TestStageCSessionChild(t *testing.T) {
	root := os.Getenv("SUPERAGENT_SESSION_TEST_DIR")
	if root == "" {
		return
	}
	ctx := sessionTestContext(t)
	action := os.Getenv("SUPERAGENT_SESSION_TEST_ACTION")
	if action == "schedule-source" {
		runScheduleSource(t, ctx, root)
		return
	}
	if action == "tool-crash" || action == "tool-recover" {
		f := defaultRecoveryFixture(root)
		if action == "tool-crash" {
			f.barrier = "after-receipt"
		}
		client, req := fixtureClient(t, f)
		s := openTestSession(t, ctx, client, root, "tool")
		if action == "tool-crash" {
			req.ID = s.NewRunID()
			run := startSessionRun(t, ctx, s, req)
			_, _ = run.Wait(ctx) // the parent kills this process before any reply
			t.Fatal("tool receipt barrier was missed")
		}
		id, err := s.PendingRun(ctx)
		if err != nil || id == "" {
			t.Fatalf("pending tool Run: %q, %v", id, err)
		}
		run, err := s.Recover(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		result := waitSessionRun(t, ctx, run, Succeeded)
		if len(result.Messages) != 4 || result.Messages[2].Tool == nil || result.Messages[2].Tool.Result.RunID != id {
			t.Fatalf("recovery did not reuse the original result: %+v", result)
		}
		commitSessionRun(t, ctx, s, id, 1)
		return
	}

	retention := time.Minute
	if action == "commit-expired" {
		retention = time.Nanosecond
	}
	client := plainSessionClient(t, root, "v1", retention)
	s := openTestSession(t, ctx, client, root, "conversation")
	stop := (&recoveryFixture{barrier: action}).stopAt
	switch action {
	case "first", "sealed", "published", "unsealed":
		req := sessionRequest(s, "first")
		result := waitSessionRun(t, ctx, startSessionRun(t, ctx, s, req), Succeeded)
		sessionLog(t, root, "session-ids", req.ID)
		if action == "unsealed" {
			return
		}
		if action == "sealed" {
			lock, err := s.store.Lock(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer lock.Close()
			idx, err := s.store.Load()
			if err != nil {
				t.Fatal(err)
			}
			if _, published, err := s.store.PrepareCommit(idx, req.ID, &result); err != nil || published {
				t.Fatalf("seal: published %v, %v", published, err)
			}
			stop("sealed")
		}
		receipt := commitSessionRun(t, ctx, s, req.ID, 1)
		stop("published")
		sessionLog(t, root, "session-commits", receipt)
	case "second":
		assertSessionVersion(t, ctx, s, 1, 2)
		req := sessionRequest(s, "second")
		waitSessionRun(t, ctx, startSessionRun(t, ctx, s, req), Succeeded)
		commitSessionRun(t, ctx, s, req.ID, 2)
		assertSessionVersion(t, ctx, s, 2, 4)
	case "reserved":
		req := sessionRequest(s, "first")
		req.Recoverable = true
		runner, err := s.runner(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.reserve(ctx, runner, req); err != nil {
			t.Fatal(err)
		}
		sessionLog(t, root, "session-ids", req.ID)
		stop("reserved")
	case "commit-expired", "recover-first", "recover-second":
		ids := readRecoveryLog[RunID](t, root, "session-ids")
		id, version := ids[len(ids)-1], uint64(1)
		if action == "commit-expired" {
			if _, err := client.Recover(ctx, id); !errors.Is(err, ErrRunNotFound) {
				t.Fatalf("original result should be expired: %v", err)
			}
		} else {
			if action == "recover-second" {
				version = 2
			}
			run, err := s.Recover(ctx, id)
			if err != nil || run.ID() != id {
				t.Fatalf("resume original: %v, %v", run, err)
			}
			waitSessionRun(t, ctx, run, Succeeded)
		}
		receipt := commitSessionRun(t, ctx, s, id, version)
		sessionLog(t, root, "session-commits", receipt)
	default:
		t.Fatalf("unknown process action %q", action)
	}
}

func TestStageCSessionC1RestartHistory(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	plainSessionClient(t, root, "v1", time.Minute)
	runSessionChild(t, root, "first", "")
	runSessionChild(t, root, "second", "")
	models := readRecoveryLog[ModelRequest](t, root, "session-models")
	if len(models) != 2 || len(models[0].Messages) != 1 || len(models[1].Messages) != 3 {
		t.Fatalf("model history after restart: %+v", models)
	}
	messages := models[1].Messages
	if messages[0].User == nil || messages[0].User.Text != "first" || messages[1].Assistant == nil ||
		messages[1].Assistant.Text != "reply first" || messages[2].User == nil || messages[2].User.Text != "second" {
		t.Fatalf("follow-up did not see ordered history and current input: %+v", messages)
	}
	client := plainSessionClient(t, root, "v1", time.Minute)
	assertSessionVersion(t, ctx, openTestSession(t, ctx, client, root, "conversation"), 2, 4)
}

func TestStageCSessionC2RecoverAdmission(t *testing.T) {
	t.Run("reserved before start", func(t *testing.T) {
		root := t.TempDir()
		plainSessionClient(t, root, "v1", time.Minute)
		runSessionChild(t, root, "reserved", "reserved")
		if got := readRecoveryLog[ModelRequest](t, root, "session-models"); len(got) != 0 {
			t.Fatalf("reservation already dispatched: %+v", got)
		}
		runSessionChild(t, root, "recover-first", "")
		if got := readRecoveryLog[ModelRequest](t, root, "session-models"); len(got) != 1 {
			t.Fatalf("expected exactly one model dispatch: %+v", got)
		}
	})
	t.Run("accepted without response", func(t *testing.T) {
		root := makeRecoveryRoot(t)
		runSessionChild(t, root, "tool-crash", "after-receipt")
		runSessionChild(t, root, "tool-recover", "")
		calls := readRecoveryLog[ReceiptKey](t, root, "invoked")
		queries := readRecoveryLog[ReceiptKey](t, root, "queries")
		models := readRecoveryLog[ModelRequest](t, root, "models")
		if len(calls) != 1 || len(queries) != 1 || calls[0] != queries[0] || len(models) != 2 ||
			models[0].RunID != calls[0].RunID || models[1].RunID != calls[0].RunID || models[1].Step != 2 {
			t.Fatalf("original dispatch, query and continuation: calls %+v, queries %+v, models %+v", calls, queries, models)
		}
	})
	t.Run("checkpoint belongs to a different request", func(t *testing.T) {
		root, ctx := t.TempDir(), sessionTestContext(t)
		client := plainSessionClient(t, root, "v1", time.Minute)
		s := openTestSession(t, ctx, client, root, "conversation")
		req := sessionRequest(s, "original")
		req.Recoverable = true
		runner, err := s.runner(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.reserve(ctx, runner, req); err != nil {
			t.Fatal(err)
		}
		changed := req
		changed.Input.Text = "different"
		run, err := client.Start(ctx, changed)
		if err != nil {
			t.Fatal(err)
		}
		waitSessionRun(t, ctx, run, Succeeded)
		if _, err := s.Recover(ctx, req.ID); !errors.Is(err, ErrSessionResultUnavailable) || !errors.Is(err, ErrStartConflict) {
			t.Fatalf("original request digest mismatch: %v", err)
		}
		assertSessionVersion(t, ctx, s, 0, 0)
		if got := readRecoveryLog[ModelRequest](t, root, "session-models"); len(got) != 1 {
			t.Fatalf("mismatched request was retried: %+v", got)
		}
	})
	t.Run("expired or replaced source", func(t *testing.T) {
		root, ctx := t.TempDir(), sessionTestContext(t)
		plainSessionClient(t, root, "v1", time.Minute)
		runSessionChild(t, root, "unsealed", "")
		id := readRecoveryLog[RunID](t, root, "session-ids")[0]
		for _, tc := range []struct {
			binding   string
			retention time.Duration
			cause     error
		}{{"v1", time.Nanosecond, ErrRunNotFound}, {"v2", time.Minute, ErrRecoveryBinding}} {
			client := plainSessionClient(t, root, tc.binding, tc.retention)
			s := openTestSession(t, ctx, client, root, "conversation")
			if _, err := s.CommitRun(ctx, id); !errors.Is(err, ErrSessionResultUnavailable) || !errors.Is(err, tc.cause) {
				t.Fatalf("source %s: %v", tc.binding, err)
			}
			if pending, err := s.PendingRun(ctx); err != nil || pending != id {
				t.Fatalf("lost original reservation: %q, %v", pending, err)
			}
			assertSessionVersion(t, ctx, s, 0, 0)
		}
		checkpoints, err := filepath.Glob(filepath.Join(root, "runs", "*.run"))
		if err != nil || len(checkpoints) != 1 {
			t.Fatalf("original checkpoint files: %v, %v", checkpoints, err)
		}
		if err := os.Rename(checkpoints[0], checkpoints[0]+".missing"); err != nil {
			t.Fatal(err)
		}
		missing := plainSessionClient(t, root, "v1", time.Minute)
		if _, err := openTestSession(t, ctx, missing, root, "conversation").Recover(ctx, id); !errors.Is(err, ErrSessionResultUnavailable) ||
			!errors.Is(err, ErrRunNotFound) {
			t.Fatalf("missing checkpoint in the original root: %v", err)
		}
		if err := os.Rename(filepath.Join(root, "runs"), filepath.Join(root, "original-runs")); err != nil {
			t.Fatal(err)
		}
		client := plainSessionClient(t, root, "v1", time.Minute)
		s := openTestSession(t, ctx, client, root, "conversation")
		if _, err := s.Recover(ctx, id); !errors.Is(err, ErrSessionResultUnavailable) || !errors.Is(err, ErrRecoveryBinding) {
			t.Fatalf("replaced storage at the same path: %v", err)
		}
		if models := readRecoveryLog[ModelRequest](t, root, "session-models"); len(models) != 1 {
			t.Fatalf("unavailable original was dispatched again: %+v", models)
		}
	})
}

func TestStageCSessionC3CrashCommit(t *testing.T) {
	for _, stage := range []string{"sealed", "published"} {
		t.Run(stage, func(t *testing.T) {
			root, ctx := t.TempDir(), sessionTestContext(t)
			client := plainSessionClient(t, root, "v1", time.Minute)
			runSessionChild(t, root, stage, stage)
			s := openTestSession(t, ctx, client, root, "conversation")
			if stage == "sealed" {
				assertSessionVersion(t, ctx, s, 0, 0)
			} else {
				assertSessionVersion(t, ctx, s, 1, 2)
			}
			done := make(chan error, 2)
			for range 2 {
				go func() {
					out, err := sessionProcessCommand(ctx, root, "commit-expired").CombinedOutput()
					if err != nil {
						t.Logf("commit child: %s", out)
					}
					done <- err
				}()
			}
			for range 2 {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			receipts := readRecoveryLog[SessionCommit](t, root, "session-commits")
			if len(receipts) != 2 || receipts[0] != receipts[1] || receipts[0].Version != 1 {
				t.Fatalf("concurrent receipts diverged: %+v", receipts)
			}
			assertSessionVersion(t, ctx, s, 1, 2)
			runSessionChild(t, root, "second", "")
			runSessionChild(t, root, "commit-expired", "")
			receipts = readRecoveryLog[SessionCommit](t, root, "session-commits")
			if len(receipts) != 3 || receipts[0] != receipts[2] {
				t.Fatalf("late receipt diverged: %+v", receipts)
			}
			assertSessionVersion(t, ctx, s, 2, 4)
			if models := readRecoveryLog[ModelRequest](t, root, "session-models"); len(models) != 2 {
				t.Fatalf("commit replay dispatched models: %+v", models)
			}
		})
	}
}

func TestStageCSessionC5CommitProtection(t *testing.T) {
	root, ctx := t.TempDir(), sessionTestContext(t)
	client := plainSessionClient(t, root, "v1", time.Minute)
	runSessionChild(t, root, "first", "")
	s := openTestSession(t, ctx, client, root, "conversation")
	first := readRecoveryLog[SessionCommit](t, root, "session-commits")[0]
	req := sessionRequest(s, "second")
	result := waitSessionRun(t, ctx, startSessionRun(t, ctx, s, req), Succeeded)
	sessionLog(t, root, "session-ids", req.ID)

	indexFiles, err := filepath.Glob(filepath.Join(root, "sessions", "*.session"))
	if err != nil || len(indexFiles) != 1 {
		t.Fatalf("session index files: %v, %v", indexFiles, err)
	}
	other := openTestSession(t, ctx, client, root, "other")
	otherCtx := context.WithValue(ctx, sessionTestSubject{}, SubjectID("other"))
	otherSubject := openTestSession(t, otherCtx, client, root, "conversation")
	for _, tc := range []struct {
		session *Session
		ctx     context.Context
	}{{other, ctx}, {otherSubject, otherCtx}, {s, otherCtx}} {
		if _, err := tc.session.CommitRun(tc.ctx, req.ID); !errors.Is(err, ErrSessionOwnership) {
			t.Fatalf("cross session or subject: %v", err)
		}
	}
	if _, _, err := s.publish(ctx, first.RunID, &Result{
		RunID: first.RunID, Outcome: Failed, Messages: []Message{{User: &UserMessage{Text: "first"}}},
	}); !errors.Is(err, ErrSessionContent) {
		t.Fatalf("conflicting terminal digest: %v", err)
	}
	lock, err := s.store.Lock(ctx)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := s.store.Load()
	if err != nil {
		lock.Close()
		t.Fatal(err)
	}
	idx.Pending.Version--
	_, _, err = s.store.PrepareCommit(idx, req.ID, &result)
	lock.Close()
	if !errors.Is(err, ErrSessionVersion) {
		t.Fatalf("stale base version: %v", err)
	}
	fault := blockSessionDelta(t, indexFiles[0], req.ID)
	t.Cleanup(fault)
	if _, err := s.CommitRun(ctx, req.ID); !errors.Is(err, ErrSessionStorage) {
		t.Fatalf("failed persistence: %v", err)
	}
	assertSessionVersion(t, ctx, s, 1, 2)
	if receipt := commitSessionRun(t, ctx, s, first.RunID, 1); first != receipt {
		t.Fatalf("old receipt changed: %+v, %+v", first, receipt)
	}
	fault()
	runSessionChild(t, root, "recover-second", "")
	second := commitSessionRun(t, ctx, s, req.ID, 2)
	receipts := readRecoveryLog[SessionCommit](t, root, "session-commits")
	if len(receipts) != 2 || receipts[1] != second {
		t.Fatalf("recovered and repeated receipt mismatch: %+v, %+v", receipts, second)
	}
	assertSessionVersion(t, ctx, s, 2, 4)
}

func blockSessionDelta(t *testing.T, indexPath string, id RunID) func() {
	t.Helper()
	var restore func() error
	if os.Geteuid() == 0 {
		target := indexPath + "." + string(id) + ".turn"
		if err := os.Mkdir(target, 0700); err != nil {
			t.Fatal(err)
		}
		restore = func() error { return os.Remove(target) }
	} else {
		dir := filepath.Dir(indexPath)
		if err := os.Chmod(dir, 0500); err != nil {
			t.Fatal(err)
		}
		restore = func() error { return os.Chmod(dir, 0700) }
	}
	return func() {
		if restore != nil {
			if err := restore(); err != nil {
				t.Error(err)
			}
			restore = nil
		}
	}
}
