package superagent_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/shubolab/superagent"
)

type meterFixture struct {
	tokens      uint64
	kind        superagent.MeasurementKind
	actual      uint64
	changeInput bool
	measured    atomic.Int32
	dispatched  atomic.Int32
}

const testProfile = "local-2026"

func (m *meterFixture) ContextLimits() superagent.ModelContextLimits {
	return superagent.ModelContextLimits{WindowTokens: 128_000}
}

func encodeMeterFixture(req superagent.ModelRequest) ([]byte, error) {
	return json.Marshal(struct {
		Instruction string
		Messages    []superagent.Message
		Tools       []superagent.ModelTool
		Summary     []superagent.ContextSummaryEntry
		MaxTokens   uint64
	}{
		req.Instruction, req.Messages, req.Tools, req.Context.Summary, req.Context.MaxOutputTokens,
	})
}

func (m *meterFixture) MeasureInput(_ context.Context, req superagent.ModelRequest) (superagent.InputTokenMeasurement, error) {
	m.measured.Add(1)
	body, err := encodeMeterFixture(req)
	if err != nil {
		return superagent.InputTokenMeasurement{}, err
	}
	return superagent.InputTokenMeasurement{Tokens: m.tokens, Kind: m.kind, ProfileVersion: testProfile,
		InputDigest: superagent.InputDigest(string(req.Model), testProfile, body)}, nil
}

func (m *meterFixture) Generate(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
	if m.changeInput {
		req.Tools[0].InputSchema = json.RawMessage(`{"type":"object","required":["path"]}`)
	}
	body, err := encodeMeterFixture(req)
	if err != nil {
		return nil, err
	}
	if err := superagent.VerifyInputDigest(req.Context, string(req.Model), testProfile, body); err != nil {
		return nil, err
	}
	m.dispatched.Add(1)
	event := completed("确定")
	event.Reply.Usage = &superagent.Usage{InputTokens: m.actual, OutputTokens: 2}
	return scripted(event), nil
}

func meteredConfig(m superagent.ModelProvider, reference string) superagent.MeshConfig {
	cfg := hostConfig(m)
	rc := cfg.LocalRunners["main"]
	rc.ContextPolicies = map[superagent.ModelRef]superagent.ModelContextPolicy{
		"chat": {MaxOutputTokens: 8000, SafetyMarginTokens: 4000, CalibratedEstimateVersion: reference},
	}
	cfg.LocalRunners["main"] = rc
	return cfg
}

func TestWindowMeterBoundaryAndBudget(t *testing.T) {
	req := request("run")
	req.Input.Text = "明确的输入"
	req.Instruction = "保留"
	body, err := json.Marshal(struct {
		Instruction string
		Messages    []superagent.Message
	}{req.Instruction, []superagent.Message{{User: &req.Input}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name        string
		tokens      uint64
		inputLimit  int
		outcome     superagent.RunOutcome
		stop        superagent.StopCode
		wantMeasure int32
		wantSend    int32
	}{
		{"just below illustrative trigger", 104399, len(body), superagent.Succeeded, "", 1, 1},
		{"at illustrative trigger before compaction", 104400, len(body), superagent.Succeeded, "", 1, 1},
		{"exact budget", 116000, len(body), superagent.Succeeded, "", 1, 1},
		{"window exceeded", 116001, len(body), superagent.BudgetExceeded, superagent.StopContextWindowLimit, 1, 0},
		{"byte budget exceeded", 104399, len(body) - 1, superagent.BudgetExceeded, superagent.StopModelInputLimit, 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			fixture := &meterFixture{tokens: tt.tokens, kind: superagent.MeasurementExact, actual: tt.tokens}
			input := req
			input.Budget.MaxModelInputBytes = pointer(uint64(tt.inputLimit))
			run := start(t, ctx, newClient(t, meteredConfig(fixture, "")), input)
			result := wait(t, ctx, run, tt.outcome)
			if fixture.measured.Load() != tt.wantMeasure || fixture.dispatched.Load() != tt.wantSend {
				t.Fatalf("counts measured %d, dispatched %d", fixture.measured.Load(), fixture.dispatched.Load())
			}
			if tt.stop != "" && (result.Stop == nil || result.Stop.Code != tt.stop || len(result.Messages) != 1) {
				t.Fatalf("result %+v, stop %+v", result, result.Stop)
			}
			if tt.stop == superagent.StopContextWindowLimit && result.Stop.LimitTokens != 116000 {
				t.Fatalf("window stop = %+v", result.Stop)
			}
		})
	}
}

func TestInputDigestChecksEveryEncodedField(t *testing.T) {
	ctx := hostContext(t, "alice")
	request := superagent.ModelRequest{
		Model: "chat", Instruction: "中国语说明", Context: &superagent.ModelContextOptions{MaxOutputTokens: 30,
			Summary: []superagent.ContextSummaryEntry{{Section: "decision", Text: "摘要", Sources: []superagent.ContextSourceRef{"ref"}}}},
		Messages: []superagent.Message{{User: &superagent.UserMessage{Text: "原文"}}},
		Tools:    []superagent.ModelTool{{Ref: "search", InputSchema: json.RawMessage(`{}`)}},
	}
	fixture := &meterFixture{tokens: 20, kind: superagent.MeasurementExact}
	m, err := fixture.MeasureInput(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	request.Context.ProfileVersion, request.Context.ExpectedInputDigest = m.ProfileVersion, m.InputDigest
	encoded, err := encodeMeterFixture(request)
	if err != nil || superagent.VerifyInputDigest(request.Context, "chat", testProfile, encoded) != nil {
		t.Fatal("unchanged serialized input did not verify")
	}
	for _, change := range []func(superagent.ModelRequest) superagent.ModelRequest{
		func(v superagent.ModelRequest) superagent.ModelRequest { v.Instruction += "改变"; return v },
		func(v superagent.ModelRequest) superagent.ModelRequest {
			v.Messages = []superagent.Message{{User: &superagent.UserMessage{Text: "更正"}}}
			return v
		},
		func(v superagent.ModelRequest) superagent.ModelRequest {
			opts := *v.Context
			opts.MaxOutputTokens++
			v.Context = &opts
			return v
		},
		func(v superagent.ModelRequest) superagent.ModelRequest {
			opts := *v.Context
			opts.Summary = nil
			v.Context = &opts
			return v
		},
	} {
		v := change(request)
		data, err := encodeMeterFixture(v)
		if err != nil || !errors.Is(superagent.VerifyInputDigest(v.Context, "chat", testProfile, data), superagent.ErrContextInputChanged) {
			t.Fatal("a changed input passed verification")
		}
	}
}

func TestInputDigestMismatchBeforeSupplierDispatch(t *testing.T) {
	ctx := hostContext(t, "alice")
	fixture := &meterFixture{tokens: 1000, kind: superagent.MeasurementExact, actual: 1100, changeInput: true}
	cfg := meteredConfig(fixture, "")
	rc := cfg.LocalRunners["main"]
	rc.AllowedTools = map[superagent.SubjectID]map[superagent.ToolRef]superagent.ToolPermission{"alice": {"probe": {}}}
	cfg.LocalRunners["main"] = rc
	cfg.LocalTools = map[superagent.ToolRef]superagent.LocalToolBinding{"probe": {
		Spec: superagent.ToolSpec{InputSchema: json.RawMessage(`{}`), Limits: superagent.ToolLimits{MaxResultBytes: 128}},
		Tool: meterTool{},
	}}
	req := request("run")
	req.Tools = []superagent.ToolRef{"probe"}
	res := wait(t, ctx, start(t, ctx, newClient(t, cfg), req), superagent.Failed)
	if res.Stop.Code != superagent.StopContextInputChanged || fixture.dispatched.Load() != 0 {
		t.Fatalf("result %+v, stop %+v", res, res.Stop)
	}
}

type meterTool struct{}

func (meterTool) Invoke(context.Context, superagent.ToolRequest) (superagent.ToolResult, error) {
	return superagent.ToolResult{}, errors.New("unexpected tool dispatch")
}

func TestEstimateDriftSharedAcrossMesh(t *testing.T) {
	for _, delta := range []uint64{4000, 4001} {
		t.Run(map[uint64]string{4000: "within margin", 4001: "beyond margin"}[delta], func(t *testing.T) {
			ctx := hostContext(t, "alice")
			fixture := &meterFixture{tokens: 1000, kind: superagent.MeasurementEstimated, actual: 1000 + delta}
			cfg := meteredConfig(fixture, testProfile)
			cfg.LocalRunners["other"] = cfg.LocalRunners["main"]
			m, err := superagent.NewMesh(cfg)
			if err != nil {
				t.Fatal(err)
			}
			first, err := superagent.NewClient(m, "main")
			if err != nil {
				t.Fatal(err)
			}
			second, err := superagent.NewClient(m, "other")
			if err != nil {
				t.Fatal(err)
			}
			got := wait(t, ctx, start(t, ctx, first, request("first")), superagent.Succeeded)
			if got.Usage == nil || got.Usage.InputTokens != fixture.actual {
				t.Fatalf("unexpected usage: %+v", got.Usage)
			}
			run := start(t, ctx, second, request("second"))
			if delta == 4001 {
				got = wait(t, ctx, run, superagent.Failed)
				if got.Stop.Code != superagent.StopContextMeasurementUnavailable || fixture.dispatched.Load() != 1 {
					t.Fatalf("stale calibration dispatched: %+v", got.Stop)
				}
			} else {
				wait(t, ctx, run, superagent.Succeeded)
			}
		})
	}
}

func TestInputMeterRequiredAndCalibrated(t *testing.T) {
	model := modelFunc(func(context.Context, superagent.ModelRequest) (superagent.ModelStream, error) {
		t.Error("unauthorized supplier call")
		return nil, superagent.ErrInvalidRequest
	})
	for name, provider := range map[string]superagent.ModelProvider{
		"no meter":                model,
		"estimate not calibrated": &meterFixture{tokens: 1000, kind: superagent.MeasurementEstimated},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := hostContext(t, "alice")
			run := start(t, ctx, newClient(t, meteredConfig(provider, "obsolete")), request("failure"))
			r := wait(t, ctx, run, superagent.Failed)
			if r.Stop.Code != superagent.StopContextMeasurementUnavailable {
				t.Fatalf("stop = %+v", r.Stop)
			}
		})
	}
}

type factoryMeter struct {
	meterFixture
	*factoryClient
}

func (f *factoryMeter) Generate(ctx context.Context, _ string, req superagent.ModelRequest) (superagent.ModelStream, error) {
	return f.meterFixture.Generate(ctx, req)
}

func (f *factoryMeter) InputMeter(id string) superagent.ModelInputMeter {
	if id != "chat-v1" {
		return nil
	}
	return &f.meterFixture
}

func TestModelFactoryPreservesOptionalInputMeter(t *testing.T) {
	ctx := hostContext(t, "alice")
	f := &factoryMeter{meterFixture: meterFixture{tokens: 1000, kind: superagent.MeasurementExact, actual: 1100},
		factoryClient: &factoryClient{close: func(context.Context) error { return nil }}}
	cfg := twoModelConfig(func(context.Context, superagent.ProviderConnection) (superagent.ModelAdapterClient, error) {
		return f, nil
	})
	b, err := superagent.NewModelBindings(ctx, cfg, credentialFunc(func(context.Context, string) (string, error) { return "local", nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(ctx)
	m := b.LocalModels()
	if _, ok := m["fast"].(superagent.ModelInputMeter); ok {
		t.Fatal("unmetered model unexpectedly claims this capability")
	}
	client := newClient(t, meteredConfig(m["chat"], ""))
	got := wait(t, ctx, start(t, ctx, client, request("fixed")), superagent.Succeeded)
	if f.measured.Load() != 1 || f.dispatched.Load() != 1 || got.Usage.InputTokens != 1100 {
		t.Fatalf("factory-backed metering result: %+v", got)
	}
}
