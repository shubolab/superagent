//go:build darwin || linux

package superagent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent"
)

type contextTrial struct {
	name    string
	history []superagent.Message
	answer  string
	input   string
}

func contextTrials(rowsPerBlock int) []contextTrial {
	var trials []contextTrial
	for _, scenario := range []struct{ name, early, correction, expected string }{
		{"early-evidence", "The confirmed gate label is COBALT-17.", "", "COBALT-17"},
		{"user-correction", "The earlier tentative label is SALT-09.", "Correction: the exact gate label is SAGE-22.", "SAGE-22"},
	} {
		history := []superagent.Message{
			{User: &superagent.UserMessage{Text: "Read only. Use the search tool exactly once. Return a single line."}},
			{Assistant: &superagent.AssistantMessage{Text: scenario.early}},
		}
		for block := range 12 {
			var filler strings.Builder
			fmt.Fprintf(&filler, "Log block %d\n", block)
			for row := range rowsPerBlock {
				fmt.Fprintf(&filler, "Record %d-%d. Build diagnostics sampled; no change to the gate decision.\n", block, row)
			}
			history = append(history, superagent.Message{Assistant: &superagent.AssistantMessage{Text: filler.String()}})
		}
		if scenario.correction != "" {
			history = append(history, superagent.Message{User: &superagent.UserMessage{Text: scenario.correction}})
		}
		trials = append(trials, contextTrial{name: scenario.name, history: history,
			answer: "status=ready label=" + scenario.expected,
			input:  "Search docs/health.txt for status. Return one line in the form status=VALUE label=LABEL, with the confirmed gate label from history."})
	}
	return trials
}

func recentHistory(history []superagent.Message, recent int) []superagent.Message {
	var out []superagent.Message
	var blockEnds []int
	for i := 0; i < len(history); {
		end := i + 1
		if a := history[i].Assistant; a != nil {
			end += len(a.ToolCalls)
		}
		blockEnds = append(blockEnds, end)
		i = end
	}
	start := 0
	if len(blockEnds) > recent {
		start = blockEnds[len(blockEnds)-recent-1]
	}
	for i, message := range history {
		if message.User != nil || i >= start {
			out = append(out, message)
		}
	}
	return out
}

type evaluationReport struct {
	Task                 string
	Strategy             string
	Outcome              superagent.RunOutcome
	Stop                 superagent.StopCode
	Correct              bool
	ConstraintsPreserved bool
	ToolExecutions       int32
	ToolReplays          int32
	ScannedBytes         uint64
	DurationMillis       int64
	Usage                *superagent.Usage `json:",omitempty"`
	EstimatedUSD         *float64          `json:",omitempty"`
	UnderestimateTokens  uint64
	MeasuredReplies      uint64
	SummaryInputBytes    uint64
	SummaryOutputBytes   uint64
	ViewSelected         bool
}

type evaluationModel struct {
	superagent.ModelProvider
	superagent.ModelInputMeter
	users     []string
	ordinary  uint64
	preserved bool
	maxUnder  uint64
	replies   uint64
}

func (p *evaluationModel) Generate(ctx context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
	if !isSummary(req) {
		p.ordinary++
		var actual []string
		for _, m := range req.Messages {
			if m.User != nil {
				actual = append(actual, m.User.Text)
			}
		}
		p.preserved = p.preserved && strings.Join(p.users, "\x00") == strings.Join(actual, "\x00")
	}
	measured, err := p.MeasureInput(ctx, req)
	if err != nil {
		return nil, err
	}
	stream, err := p.ModelProvider.Generate(ctx, req)
	if err != nil {
		return nil, err
	}
	return evaluationStream{ModelStream: stream, estimate: measured.Tokens, owner: p}, nil
}

type evaluationStream struct {
	superagent.ModelStream
	estimate uint64
	owner    *evaluationModel
}

func (s evaluationStream) Recv() (superagent.ModelEvent, error) {
	event, err := s.ModelStream.Recv()
	if err == nil && event.Kind == superagent.ModelCompleted && event.Reply != nil && event.Reply.Usage != nil {
		s.owner.replies++
		if actual := event.Reply.Usage.InputTokens; actual > s.estimate {
			s.owner.maxUnder = max(s.owner.maxUnder, actual-s.estimate)
		}
	}
	return event, err
}

type evaluationOptions struct {
	OutputTokens            uint64
	SafetyMargin            uint64
	EstimateVersion         string
	InputPrice, OutputPrice float64
}

func compareContextStrategies(t *testing.T, provider superagent.ModelProvider, cases []contextTrial, opts evaluationOptions) []evaluationReport {
	t.Helper()
	meter, ok := provider.(superagent.ModelInputMeter)
	if !ok {
		t.Fatal("comparison requires a metered provider")
	}
	var reports []evaluationReport
	for _, task := range cases {
		for _, name := range []string{"full", "recent", "adaptive"} {
			ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), callerKey{}, superagent.SubjectID("alice")), 100*time.Second)
			report := evaluateContextStrategy(t, ctx, provider, meter, task, name, opts)
			cancel()
			reportJSON, _ := json.Marshal(report)
			t.Log(string(reportJSON))
			reports = append(reports, report)
		}
	}
	return reports
}

func evaluateContextStrategy(t *testing.T, ctx context.Context, provider superagent.ModelProvider, meter superagent.ModelInputMeter,
	task contextTrial, name string, opts evaluationOptions) evaluationReport {
	t.Helper()
	fixture := newSearchFixture(t)
	req := fixture.request()
	req.ID = superagent.RunID(task.name + "-" + name)
	req.Instruction = "Respect user constraints and corrections. Use tools only as authorized."
	req.Input.Text = task.input
	req.History = task.history
	req.Tools = append(req.Tools, sourceRef)

	observed := &evaluationModel{ModelProvider: provider, ModelInputMeter: meter, preserved: true}
	for _, m := range task.history {
		if m.User != nil {
			observed.users = append(observed.users, m.User.Text)
		}
	}
	observed.users = append(observed.users, req.Input.Text)
	policy := superagent.ModelContextPolicy{MaxOutputTokens: opts.OutputTokens, SafetyMarginTokens: opts.SafetyMargin,
		CalibratedEstimateVersion: opts.EstimateVersion}
	source := superagent.NewContextSourceTool()
	switch name {
	case "recent":
		req.History = recentHistory(task.history, 3)
	case "adaptive":
		policy.Compaction = &superagent.ContextCompactionPolicy{
			TriggerRatio: .90, TargetRatio: .70, SummaryMaxTokens: opts.OutputTokens,
			SummaryMaxBytes: 8192, MinSavedTokens: 8, MaxCompactionsPerRun: 3, SourcePageBytes: 4096, SourceTool: sourceRef,
		}
	}
	if name != "adaptive" {
		// Keep the model-visible tool schema fixed across these runs.
		source.Spec.NeedsContextSource = false
		source.Tool = compactTool(func(_ context.Context, req superagent.ToolRequest) (superagent.ToolResult, error) {
			return superagent.ToolResult{RunID: req.RunID, CallID: req.CallID, State: superagent.ToolExecutionNotExecuted,
				Reason: &superagent.ToolReason{Code: "context_source_unavailable"}}, nil
		})
	}
	cfg := fixture.cfg
	cfg.LocalModels["chat"] = observed
	cfg.LocalTools[sourceRef] = source
	rc := cfg.LocalRunners["main"]
	rc.AllowedTools["alice"][sourceRef] = superagent.ToolPermission{}
	rc.DefaultBudget.Timeout = pointer(90 * time.Second)
	rc.DefaultBudget.MaxModelRequests = pointer(uint64(10))
	rc.DefaultBudget.MaxToolRequests = pointer(uint64(4))
	rc.DefaultBudget.MaxModelInputBytes = pointer(uint64(32 << 20))
	rc.DefaultBudget.MaxOutputBytes = pointer(uint64(65536))
	rc.BudgetCaps = rc.DefaultBudget
	rc.ContextPolicies = map[superagent.ModelRef]superagent.ModelContextPolicy{"chat": policy}
	cfg.LocalRunners["main"] = rc

	started := time.Now()
	run := start(t, ctx, newClient(t, cfg), req)
	result, err := run.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	report := evaluationReport{Task: task.name, Strategy: name, Outcome: result.Outcome,
		Correct:              result.Answer != nil && strings.TrimSpace(result.Answer.Text) == task.answer,
		ConstraintsPreserved: observed.preserved && observed.ordinary > 0,
		ToolExecutions:       fixture.hits.Load(), ScannedBytes: fixture.env.scanned.Load(),
		DurationMillis: time.Since(started).Milliseconds(), Usage: result.Usage,
		UnderestimateTokens: observed.maxUnder, MeasuredReplies: observed.replies,
	}
	report.ToolReplays = max(report.ToolExecutions-1, 0)
	if result.Stop != nil {
		report.Stop = result.Stop.Code
	}
	if usage := result.Usage; usage != nil {
		total := (float64(usage.InputTokens)*opts.InputPrice + float64(usage.OutputTokens)*opts.OutputPrice) / 1e6
		report.EstimatedUSD = &total
	}
	for _, c := range result.ContextCompactions {
		report.SummaryInputBytes += c.SummaryInputBytes
		report.SummaryOutputBytes += c.SummaryOutputBytes
		report.ViewSelected = report.ViewSelected || c.Selection == "shortened" || c.Selection == "summary"
	}
	return report
}

type liveEvaluationProfile struct {
	ModelID, ProfileVersion                                        string
	Kind                                                           superagent.MeasurementKind
	WindowTokens, MaxInputTokens, OutputTokens, SafetyMarginTokens uint64
	RowsPerBlock                                                   int
	InputUSDPerMillion, OutputUSDPerMillion                        float64
	MeterCommand                                                   []string
}

func loadEvaluationProfile(raw []byte) (liveEvaluationProfile, error) {
	var cfg liveEvaluationProfile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&cfg) != nil || !json.Valid(raw) || cfg.ModelID == "" || cfg.ProfileVersion == "" ||
		(cfg.Kind != superagent.MeasurementExact && cfg.Kind != superagent.MeasurementEstimated) ||
		cfg.WindowTokens == 0 || cfg.OutputTokens == 0 || cfg.OutputTokens >= cfg.WindowTokens ||
		cfg.RowsPerBlock < 1 || cfg.RowsPerBlock > 20000 ||
		cfg.InputUSDPerMillion <= 0 || cfg.OutputUSDPerMillion <= 0 ||
		math.IsInf(cfg.InputUSDPerMillion+cfg.OutputUSDPerMillion, 0) || len(cfg.MeterCommand) == 0 || cfg.MeterCommand[0] == "" {
		return liveEvaluationProfile{}, errors.New("invalid comparison profile")
	}
	limit := cfg.WindowTokens - cfg.OutputTokens
	if cfg.MaxInputTokens != 0 {
		limit = min(limit, cfg.MaxInputTokens)
	}
	if cfg.SafetyMarginTokens >= limit {
		return liveEvaluationProfile{}, errors.New("comparison profile leaves no input window")
	}
	return cfg, nil
}

func TestAnthropicLiveCompactionComparison(t *testing.T) {
	if os.Getenv("SUPERAGENT_ANTHROPIC_EVAL") != "1" {
		t.Skip("explicit opt-in required for paid supplier requests")
	}
	// Keep existing relative profile and meter paths rooted at the repository.
	t.Chdir("../..")
	path, key := os.Getenv("SUPERAGENT_ANTHROPIC_EVAL_PROFILE"), os.Getenv("ANTHROPIC_API_KEY")
	if path == "" || key == "" {
		t.Fatal("configure SUPERAGENT_ANTHROPIC_EVAL_PROFILE and ANTHROPIC_API_KEY")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := loadEvaluationProfile(raw)
	if err != nil {
		t.Fatal(err)
	}
	meter := superagent.AnthropicInputProfile{
		Limits: superagent.ModelContextLimits{WindowTokens: cfg.WindowTokens, MaxInputTokens: cfg.MaxInputTokens},
		Kind:   cfg.Kind, Version: cfg.ProfileVersion,
		CountTokens: func(ctx context.Context, body []byte) (uint64, error) {
			cmd := exec.CommandContext(ctx, cfg.MeterCommand[0], cfg.MeterCommand[1:]...)
			cmd.Stdin = bytes.NewReader(body)
			// The process only needs serialized input. Keep the supplier credential in the client.
			env := os.Environ()
			cmd.Env = make([]string, 0, len(env))
			for _, v := range env {
				if !strings.HasPrefix(v, "ANTHROPIC_API_KEY=") {
					cmd.Env = append(cmd.Env, v)
				}
			}
			raw, err := cmd.Output()
			if err != nil {
				return 0, errors.New("local input meter failed")
			}
			return strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
		},
	}
	bindings, err := superagent.NewModelBindings(t.Context(), superagent.ModelFactoryConfig{
		Providers: map[superagent.ProviderRef]superagent.ProviderConfig{"live": {Adapter: "anthropic", CredentialRef: "test-api-key", Endpoint: os.Getenv("SUPERAGENT_ANTHROPIC_BASE_URL")}},
		Models:    map[superagent.ModelRef]superagent.ModelBindingConfig{"chat": {Provider: "live", ModelID: cfg.ModelID}},
		Adapters: map[string]superagent.ModelAdapter{"anthropic": liveAnthropicAdapter(cfg.OutputTokens,
			map[string]superagent.AnthropicInputProfile{cfg.ModelID: meter})},
	}, credentialFunc(func(context.Context, string) (string, error) { return key, nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer bindings.Close(context.Background())
	reports := compareContextStrategies(t, bindings.LocalModels()["chat"], contextTrials(cfg.RowsPerBlock), evaluationOptions{
		OutputTokens: cfg.OutputTokens, SafetyMargin: cfg.SafetyMarginTokens, EstimateVersion: cfg.ProfileVersion,
		InputPrice: cfg.InputUSDPerMillion, OutputPrice: cfg.OutputUSDPerMillion,
	})
	for _, report := range reports {
		if report.MeasuredReplies == 0 || report.UnderestimateTokens > cfg.SafetyMarginTokens || !report.ConstraintsPreserved ||
			report.ToolReplays != 0 || (report.Strategy != "recent" && (!report.Correct || report.ToolExecutions != 1)) ||
			(report.Strategy == "adaptive" && !report.ViewSelected) {
			t.Errorf("acceptance failed for %s/%s", report.Task, report.Strategy)
		}
	}
}

func TestContextCompactionComparisonLocal(t *testing.T) {
	var summaries atomic.Uint64
	m := &compactFixture{measure: func(req superagent.ModelRequest) uint64 {
		if isSummary(req) {
			return 24
		}
		tokens := uint64(12 + 2*len(req.Context.Summary))
		for _, item := range req.Messages {
			if item.Assistant != nil && strings.HasPrefix(item.Assistant.Text, "Log block") {
				tokens += 14
			}
			if item.Tool != nil {
				tokens += 4
			}
		}
		return tokens
	}}
	m.modelFunc = func(_ context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
		var event superagent.ModelEvent
		switch {
		case isSummary(req):
			summaries.Add(1)
			sources, err := summarySources(req)
			if err != nil {
				return nil, err
			}
			refs := make([]superagent.ContextSourceRef, len(sources))
			text := "Routine logs"
			for i, source := range sources {
				refs[i] = source.Ref
				if strings.Contains(source.Original, "COBALT-17") || strings.Contains(source.Original, "SALT-09") {
					text = source.Original
				}
			}
			compact, _ := json.Marshal([]superagent.ContextSummaryEntry{{Section: "effects", Text: text, Sources: refs}})
			event = completed(string(compact))
		case req.Messages[len(req.Messages)-1].Tool == nil:
			event = searchCompleted("", searchCall("search-once", "docs/health.txt"))
		default:
			label := "unknown"
			for _, compacted := range req.Context.Summary {
				if strings.Contains(compacted.Text, "COBALT-17") {
					label = "COBALT-17"
				}
			}
			for _, message := range req.Messages {
				if message.Assistant != nil && strings.Contains(message.Assistant.Text, "COBALT-17") {
					label = "COBALT-17"
				}
				if message.User != nil && strings.Contains(message.User.Text, "SAGE-22") {
					label = "SAGE-22"
				}
			}
			event = completed("status=ready label=" + label)
		}
		event.Reply.Usage = &superagent.Usage{InputTokens: m.measure(req), OutputTokens: 1}
		return scripted(event), nil
	}
	reports := compareContextStrategies(t, m, contextTrials(2), evaluationOptions{OutputTokens: 16, InputPrice: 2, OutputPrice: 4})
	if len(reports) != 6 || summaries.Load() != 2 {
		t.Fatalf("reports %d, summary calls %d", len(reports), summaries.Load())
	}
	for _, report := range reports {
		correct := report.Task != "early-evidence" || report.Strategy != "recent"
		if report.Correct != correct || !report.ConstraintsPreserved || report.ToolExecutions != 1 || report.ToolReplays != 0 ||
			report.ScannedBytes != 13 || report.Outcome != superagent.Succeeded || report.Usage == nil ||
			report.EstimatedUSD == nil || *report.EstimatedUSD == 0 || report.UnderestimateTokens != 0 ||
			(report.Strategy == "adaptive") != (report.SummaryInputBytes > 0 && report.SummaryOutputBytes > 0) {
			t.Fatalf("invalid comparison report: %+v", report)
		}
	}
}

func TestCompactionEvaluationProfileRequiresValidMeterAndWindow(t *testing.T) {
	const valid = `{"ModelID":"test","ProfileVersion":"v1","Kind":"estimated","WindowTokens":220,"OutputTokens":20,"SafetyMarginTokens":10,"RowsPerBlock":5,"InputUSDPerMillion":1,"OutputUSDPerMillion":2,"MeterCommand":["/tmp/test-meter"]}`
	if _, err := loadEvaluationProfile([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		strings.Replace(valid, `"v1"`, `""`, 1),
		strings.Replace(valid, `"estimated"`, `"unknown"`, 1),
		strings.Replace(valid, `["/tmp/test-meter"]`, `[]`, 1),
		strings.Replace(valid, "220", "25", 1),
		strings.Replace(valid, `"RowsPerBlock":5`, `"RowsPerBlock":-1`, 1),
		valid + `{}`,
	} {
		if _, err := loadEvaluationProfile([]byte(invalid)); err == nil {
			t.Fatalf("accepted invalid profile %q", invalid)
		}
	}
}
