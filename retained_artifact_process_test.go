package superagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/run"
)

const retainedArtifactTool = "write"
const retainedArtifactPhase = "artifact-phase-"

var diskArtifactPayload = bytes.Repeat([]byte("retained artifact: confirmed content across processes\n"), 3100)

type retainedProcessConfig struct {
	Root, Role, Barrier string
	Takeover, Old       bool
	Timeout, Retention  time.Duration
	Window              time.Duration
}

func (c retainedProcessConfig) storeConfig() LocalArtifactConfig {
	retention := c.Retention
	if retention == 0 {
		retention = 25 * time.Second
	}
	return LocalArtifactConfig{Directory: filepath.Join(c.Root, "artifacts"),
		MaxArtifactBytes: 256 << 10, MaxStoredBytes: 512 << 10, MaxArtifacts: 4,
		MaxPageBytes: 48 << 10, Retention: retention}
}

func (c retainedProcessConfig) barrier(ctx context.Context, point string) error {
	if c.Barrier != point {
		return nil
	}
	if err := writeTakeoverMarker(c.Root, retainedArtifactPhase+point, "reached"); err != nil {
		return err
	}
	return waitTakeoverMarker(ctx, c.Root, retainedArtifactPhase+"release-"+point)
}

func (c retainedProcessConfig) tool(f *recoveryFixture) Tool {
	return deliveryTool(func(ctx context.Context, req ToolRequest) (ToolResult, error) {
		key, ok := InvocationReceiptKey(ctx)
		if !ok {
			return ToolResult{}, ErrInvalidRequest
		}
		f.append("artifact-invocations", key)
		writer, err := OpenToolArtifact(ctx, req)
		if err != nil {
			return ToolResult{}, err
		}
		defer writer.Close()
		for offset := 0; offset < len(diskArtifactPayload); {
			// Deliberately split integrity blocks across tool writes.
			end := min(offset+37801, len(diskArtifactPayload))
			n, err := writer.Write(diskArtifactPayload[offset:end])
			if err != nil || n != end-offset {
				return ToolResult{}, errors.Join(err, io.ErrShortWrite)
			}
			if offset == 0 {
				if err := c.barrier(context.WithoutCancel(ctx), "writing"); err != nil {
					return ToolResult{}, err
				}
			}
			offset = end
		}
		if err := c.barrier(context.WithoutCancel(ctx), "before-seal"); err != nil {
			return ToolResult{}, err
		}
		ref, err := writer.Complete()
		if c.Old && c.Barrier == "before-seal" {
			f.append("artifact-old-seal-denied", err != nil && ref.ID == "")
		}
		if err != nil {
			return ToolResult{}, err
		}
		f.append("artifact-original-ref", ref)
		if err := c.barrier(context.WithoutCancel(ctx), "sealed"); err != nil {
			return ToolResult{}, err
		}
		return ToolResult{RunID: req.RunID, CallID: req.CallID, State: ToolExecutionKnown, Completion: &ToolCompletion{
			Outcome: ToolSucceeded, Content: "captured", Artifacts: []ArtifactRef{ref},
		}}, nil
	})
}

type retainedFixtureTransport struct {
	mesh.MeshTransport
	cfg retainedProcessConfig
	ctx context.Context
	f   *recoveryFixture
}

func (m retainedFixtureTransport) Call(ctx context.Context, req mesh.MeshRequest) (mesh.MeshReply, error) {
	if req.Operation == mesh.MeasureModelInput && req.Model.Step == 2 {
		if err := m.cfg.barrier(m.ctx, "batch"); err != nil {
			return mesh.MeshReply{}, err
		}
	}
	if req.Operation == mesh.SettleArtifactOutput && len(req.Environment.Artifacts) != 0 && m.cfg.Old {
		if err := m.cfg.barrier(m.ctx, "old-publish"); err != nil {
			return mesh.MeshReply{}, err
		}
		// Exercise the shared fence even when the old operation's context has already stopped.
		_, err := m.MeshTransport.Call(context.WithoutCancel(ctx), req)
		m.f.append("artifact-old-publish-denied", errors.Is(err, ErrRecoveryOwnershipLost))
		return mesh.MeshReply{}, err
	}
	reply, err := m.MeshTransport.Call(ctx, req)
	if req.Operation == mesh.QueryReceipt {
		m.f.append("artifact-queries", req.ReceiptKey)
		if m.cfg.Barrier == "cancel" {
			m.f.append("artifact-cancel-queries", req.ReceiptKey)
			if err := writeTakeoverMarker(m.cfg.Root, retainedArtifactPhase+"query", "attempted"); err != nil {
				return mesh.MeshReply{}, err
			}
		}
	}
	if err != nil {
		return reply, err
	}
	if req.Operation == mesh.Invoke {
		err = m.cfg.barrier(m.ctx, "receipt")
	}
	if req.Operation == mesh.SettleArtifactOutput && len(req.Environment.Artifacts) == 1 {
		err = m.cfg.barrier(m.ctx, "published")
	}
	return reply, err
}

type retainedTestCallerKey struct{}

func retainedProcessClient(t *testing.T, ctx context.Context, cfg retainedProcessConfig) *Client {
	t.Helper()
	f := defaultRecoveryFixture(cfg.Root)
	store, err := NewLocalRetainedArtifactStore(cfg.storeConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	tool, err := NewLocalReceiptTool(filepath.Join(cfg.Root, "receipts"), cfg.tool(f))
	if err != nil {
		t.Fatal(err)
	}
	timeout, window := cfg.Timeout, cfg.Window
	if timeout == 0 {
		timeout = 12 * time.Second
	}
	if window == 0 {
		window = 550 * time.Millisecond
		if cfg.Takeover {
			window = 3 * time.Second
		}
	}
	budget := Budget{Timeout: address(timeout), MaxModelRequests: address(uint64(2)), MaxModelInputBytes: address(uint64(8192)),
		MaxOutputBytes: address(uint64(16384)), MaxToolRequests: address(uint64(1)), MaxToolRawOutputBytes: address(uint64(256 << 10))}
	rc := RunnerConfig{AllowedModels: map[SubjectID][]ModelRef{"owner": {"model"}},
		AllowedTools:  map[SubjectID]map[ToolRef]ToolPermission{"owner": {retainedArtifactTool: {AllowArtifacts: true}}},
		DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 4096, ResultRetention: time.Minute,
		ContextPolicies: map[ModelRef]ModelContextPolicy{"model": {MaxOutputTokens: 20}},
		Recovery:        &RecoveryConfig{Directory: filepath.Join(cfg.Root, "runs"), BindingVersion: "v1", ReconcileTimeout: window}}
	if cfg.Takeover {
		rc.Recovery.Takeover = &TakeoverConfig{LeaseDuration: 600 * time.Millisecond}
	}
	models := map[ModelRef]ModelProvider{"model": f}
	tools := map[ToolRef]LocalToolBinding{retainedArtifactTool: {Tool: tool, Spec: ToolSpec{
		InputSchema: json.RawMessage(`{"type":"object"}`), NeedsArtifactOutput: true,
		Limits: ToolLimits{MaxResultBytes: 1024, MaxRawOutputBytes: 256 << 10},
	}}}
	caller := func(ctx context.Context) (SubjectID, error) {
		if subject, ok := ctx.Value(retainedTestCallerKey{}).(SubjectID); ok {
			return subject, nil
		}
		return "owner", nil
	}
	if cfg.Barrier == "" {
		m, err := NewMesh(MeshConfig{LocalRunners: map[RunnerRef]RunnerConfig{"runner": rc}, LocalModels: models,
			LocalTools: tools, LocalArtifacts: store, ResolveCaller: caller})
		if err != nil {
			t.Fatal(err)
		}
		client, err := NewClient(m, "runner")
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	var transport retainedFixtureTransport
	r, err := run.New(rc, map[ModelRef]struct{}{"model": {}}, func(ctx context.Context, subject SubjectID, req ModelRequest) (ModelStream, error) {
		return mesh.OpenModel(ctx, transport, subject, req)
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Old {
		r.SetTakeoverClock(func() (time.Time, error) {
			raw, err := os.ReadFile(filepath.Join(cfg.Root, retainedArtifactPhase+"freeze"))
			if errors.Is(err, os.ErrNotExist) {
				return time.Now(), nil
			}
			if err != nil {
				return time.Time{}, err
			}
			ns, err := strconv.ParseInt(string(raw), 10, 64)
			return time.Unix(0, ns), err
		})
	}
	local, err := mesh.NewLocal(map[RunnerRef]mesh.RunnerEndpoint{"runner": r}, models, caller)
	if err != nil {
		t.Fatal(err)
	}
	if err := local.SetArtifactStore(store); err != nil {
		t.Fatal(err)
	}
	if err := local.SetComponents(tools, nil); err != nil {
		t.Fatal(err)
	}
	transport = retainedFixtureTransport{MeshTransport: local, cfg: cfg, ctx: ctx, f: f}
	r.SetRetainedArtifacts(true)
	r.SetTakeoverArtifacts(true)
	r.SetArtifactStoreID(store.(contract.DurableArtifactStore).ArtifactStoreID())
	if err := r.ConfigureTools("runner", transport, rc, tools, nil, true); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(&Mesh{local: local}, "runner")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestRetainedArtifactProcess(t *testing.T) {
	raw := os.Getenv("SUPERAGENT_RETAINED_ARTIFACT_TEST")
	if raw == "" {
		return
	}
	var cfg retainedProcessConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	client := retainedProcessClient(t, ctx, cfg)
	var handle *Run
	var err error
	if cfg.Role == "start" {
		handle, err = client.Start(ctx, StartRequest{ID: recoveryID, Model: "model", Tools: []ToolRef{retainedArtifactTool},
			Input: UserMessage{Text: "capture"}, Recoverable: true, Takeover: cfg.Takeover})
	} else {
		handle, err = stageBRecover(ctx, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTakeoverMarker(cfg.Root, retainedArtifactPhase+"accepted-"+cfg.Role, "yes"); err != nil {
		t.Fatal(err)
	}
	if cfg.Barrier == "cancel" {
		if err := waitTakeoverMarker(ctx, cfg.Root, retainedArtifactPhase+"cancel"); err != nil {
			t.Fatal(err)
		}
		if err := handle.Cancel(ctx); err != nil {
			t.Fatal(err)
		}
	}
	result, err := handle.Wait(ctx)
	if cfg.Old {
		if !errors.Is(err, ErrRecoveryOwnershipLost) {
			t.Fatalf("old runner Wait: %+v (%v)", result, err)
		}
		// Keep the old process alive while the parent releases the late operation.
		if err := waitTakeoverMarker(ctx, cfg.Root, retainedArtifactPhase+"finish-old"); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	defaultRecoveryFixture(cfg.Root).append("artifact-result-"+cfg.Role, result)
}

func launchRetainedProcess(t *testing.T, cfg retainedProcessConfig, role string) stageBProcess {
	t.Helper()
	cfg.Role = role
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return launchStageBProcess(t, "TestRetainedArtifactProcess", "SUPERAGENT_RETAINED_ARTIFACT_TEST="+string(raw))
}

func waitRetainedPhase(t *testing.T, root, point string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	if err := waitTakeoverMarker(ctx, root, retainedArtifactPhase+point); err != nil {
		t.Fatal(err)
	}
}

func retainedResult(t *testing.T, root, role string) Result {
	t.Helper()
	all := readRecoveryLog[Result](t, root, "artifact-result-"+role)
	if len(all) != 1 {
		t.Fatalf("%s result count: %d", role, len(all))
	}
	return all[0]
}

func retainedOriginalRef(t *testing.T, root string) ArtifactRef {
	t.Helper()
	all := readRecoveryLog[ArtifactRef](t, root, "artifact-original-ref")
	if len(all) != 1 {
		t.Fatalf("original refs: %+v", all)
	}
	return all[0]
}

func assertArtifactUnavailable(t *testing.T, ctx context.Context, r *Run, ref ArtifactRef, offset, max uint64) {
	t.Helper()
	page, err := r.ReadArtifact(ctx, ref, offset, max)
	if !errors.Is(err, ErrArtifactUnavailable) || len(page.Content) != 0 {
		t.Fatalf("read supplied content or wrong error for bad artifact: %+v (%v)", page, err)
	}
}

func TestRetainedArtifactPublicationCrash(t *testing.T) {
	requireLocalRecovery(t)
	for _, barrier := range []string{"writing", "sealed", "receipt", "published"} {
		t.Run(barrier, func(t *testing.T) {
			cfg := retainedProcessConfig{Root: makeRecoveryRoot(t), Barrier: barrier}
			child := launchRetainedProcess(t, cfg, "start")
			waitRetainedPhase(t, cfg.Root, barrier)
			before := readRecoveryCheckpoint(t, cfg.Root)
			journal, err := os.ReadFile(evidenceFixturePath(t, cfg.Root, "receipts/*.receipt"))
			var receipt struct{ Result *ToolResult }
			if err != nil || json.Unmarshal(journal, &receipt) != nil || before.Intent == nil ||
				before.Intent.Key.CallID != "c1" || len(before.ToolResults) != 0 {
				t.Fatalf("wrong crash checkpoint/receipt: %+v, %q (%v)", before, journal, err)
			}
			known := barrier == "receipt" || barrier == "published"
			catalogPath := filepath.Join(cfg.Root, "artifacts", "catalog.json")
			catalog, err := os.ReadFile(catalogPath)
			if err != nil {
				t.Fatal(err)
			}
			if (receipt.Result != nil) != known {
				t.Fatal("wrong durable receipt boundary")
			}
			if barrier == "writing" {
				body, err := os.Stat(evidenceFixturePath(t, cfg.Root, "artifacts/*.body"))
				if err != nil || body.Size() != 37801 {
					t.Fatalf("missing partial private write: %v", err)
				}
			}
			child.stop()
			cfg.Barrier = ""
			client := retainedProcessClient(t, t.Context(), cfg)
			var ref ArtifactRef
			if barrier != "writing" {
				ref = retainedOriginalRef(t, cfg.Root)
				if barrier == "published" {
					page, err := client.Run(recoveryID).ReadArtifact(t.Context(), ref, 0, 19)
					if err != nil || !bytes.Equal(page.Content, diskArtifactPayload[:19]) {
						t.Fatalf("device publication was not durable: %+v (%v)", page, err)
					}
				} else {
					assertArtifactUnavailable(t, t.Context(), client.Run(recoveryID), ref, 0, 1)
				}
			} else if len(readRecoveryLog[ArtifactRef](t, cfg.Root, "artifact-original-ref")) != 0 {
				t.Fatal("partial writer returned a reference")
			}
			resume := launchRetainedProcess(t, cfg, "resume")
			resume.wait()
			result := retainedResult(t, cfg.Root, "resume")
			if known {
				checkRetainedSettlement(t, cfg.Root, before, result, ref)
				handle, err := client.Recover(t.Context(), recoveryID)
				if err != nil {
					t.Fatal(err)
				}
				verifyRetainedPages(t, handle, ref, cfg.storeConfig().MaxPageBytes)
				lines, err := os.ReadFile(evidenceFixturePath(t, cfg.Root, "runs/*.run"))
				if err != nil || bytes.Contains(lines, diskArtifactPayload[:70]) || bytes.Contains(journal, diskArtifactPayload[:70]) ||
					!bytes.Contains(lines, []byte(ref.Version)) || !bytes.Contains(journal, []byte(ref.Version)) {
					t.Fatalf("durable records did not contain only the original reference (%v)", err)
				}
				published, err := os.ReadFile(catalogPath)
				if err != nil || (barrier == "published" && !bytes.Equal(catalog, published)) ||
					(barrier == "receipt" && bytes.Equal(catalog, published)) {
					t.Fatalf("publication was skipped or repeated (%v)", err)
				}
				t.Logf("phase=%s writer_pid=%d recovery_pid=%d bytes=%d checkpoint_bytes=%d receipt_bytes=%d sha256=%s", barrier,
					child.pid, resume.pid, ref.SizeBytes, len(lines), len(journal), ref.SHA256)
			} else {
				checkRetainedUnknown(t, cfg.Root, before, result, StopReconcileDeadline)
			}
			validateOneArtifactExecution(t, cfg.Root, before)
		})
	}
}

func checkRetainedSettlement(t *testing.T, root string, before fixtureCheckpoint, result Result, ref ArtifactRef) {
	t.Helper()
	models := readRecoveryLog[ModelRequest](t, root, "models")
	tools := stageBToolResults(t, result.Messages)
	after := readRecoveryCheckpoint(t, root)
	if result.Outcome != Succeeded || len(tools) != 1 || len(models) != 2 || after.Intent != nil ||
		tools[0].Completion == nil || len(tools[0].Completion.Artifacts) != 1 || !reflect.DeepEqual(ref, tools[0].Completion.Artifacts[0]) ||
		tools[0].Usage == nil || tools[0].Usage.RawOutputBytes == nil || *tools[0].Usage.RawOutputBytes != uint64(len(diskArtifactPayload)) ||
		after.Balance.Raw != before.Balance.Raw-ref.SizeBytes || after.Balance.ToolRequests != before.Balance.ToolRequests ||
		!reflect.DeepEqual(stageBToolResults(t, models[1].Messages), tools) ||
		models[1].MaxOutputBytes != before.Balance.Output-jsonSize(t, tools[0]) {
		t.Fatalf("wrong result, ref or budget on recovery: %+v, checkpoint %+v", result, after)
	}
}

func checkRetainedUnknown(t *testing.T, root string, before fixtureCheckpoint, result Result, trigger StopCode) {
	t.Helper()
	after := readRecoveryCheckpoint(t, root)
	if result.Outcome != ToolOutcomeUnknown || result.Stop == nil || result.Stop.CallID != "c1" || result.Stop.Trigger != trigger ||
		len(result.Messages) != 1 || len(readRecoveryLog[ModelRequest](t, root, "models")) != 1 ||
		after.Balance.Raw != before.Balance.Raw-before.Intent.Limits.MaxRawOutputBytes ||
		after.Balance.Output != before.Balance.Output-before.Intent.Limits.MaxResultBytes {
		t.Fatalf("missing artifact advanced the original Run or undercharged: %+v, checkpoint %+v", result, after)
	}
}

func validateOneArtifactExecution(t *testing.T, root string, before fixtureCheckpoint) {
	t.Helper()
	executed := readRecoveryLog[ReceiptKey](t, root, "artifact-invocations")
	if len(executed) != 1 || before.Intent == nil || executed[0] != before.Intent.Key {
		t.Fatalf("original tool was replayed or changed identity: %+v", executed)
	}
}

func verifyRetainedPages(t *testing.T, handle *Run, ref ArtifactRef, pageMax uint64) {
	t.Helper()
	var body []byte
	for offset := uint64(0); ; {
		page, err := handle.ReadArtifact(t.Context(), ref, offset, ref.SizeBytes)
		size := min(pageMax, ref.SizeBytes-offset)
		if err != nil || uint64(len(page.Content)) != size || page.NextOffset != offset+size ||
			page.EOF != (page.NextOffset == ref.SizeBytes) {
			t.Fatalf("page at %d: length %d next %d eof %t (%v)", offset, len(page.Content), page.NextOffset, page.EOF, err)
		}
		body = append(body, page.Content...)
		offset = page.NextOffset
		if page.EOF {
			break
		}
	}
	if ref.SizeBytes <= 2*ArtifactMaxBlockBytes || !bytes.Equal(body, diskArtifactPayload) || uint64(len(body)) != ref.SizeBytes ||
		contract.HashBytes(body) != ref.SHA256 {
		t.Fatal("reopened artifact is not the original complete data")
	}
	cross := uint64(ArtifactMaxBlockBytes - 23)
	page, err := handle.ReadArtifact(t.Context(), ref, cross, 40)
	if err != nil || !bytes.Equal(page.Content, body[cross:cross+40]) {
		t.Fatalf("non-aligned cross-block read: %+v (%v)", page, err)
	}
	last, err := handle.ReadArtifact(t.Context(), ref, ref.SizeBytes, 1)
	if err != nil || len(last.Content) != 0 || !last.EOF || last.NextOffset != ref.SizeBytes {
		t.Fatalf("EOF boundary: %+v (%v)", last, err)
	}
}
