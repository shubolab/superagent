//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	sdk "github.com/shubolab/superagent"
	"github.com/shubolab/superagent/internal/durable"
)

type searchProbe struct {
	Kind  string
	Run   sdk.RunID
	Key   sdk.ReceiptKey
	Bytes uint64
	Size  int
}

type searchFault struct{ dir, mode string }

func (f searchFault) record(kind string, ctx context.Context, id sdk.RunID) {
	key, _ := sdk.InvocationReceiptKey(ctx)
	appendProbe(f.dir, searchProbe{Kind: kind, Run: id, Key: key})
}

func (f searchFault) park(kind string, id sdk.RunID) {
	f.record(kind, context.Background(), id)
	// The parent exits this subprocess at the selected persistent boundary.
	time.Sleep(time.Minute)
}

type meteredSearch struct {
	sdk.Tool
	fault searchFault
}

func (t meteredSearch) Invoke(ctx context.Context, req sdk.ToolRequest) (sdk.ToolResult, error) {
	t.fault.record("execute", ctx, req.RunID)
	result, err := t.Tool.Invoke(ctx, req)
	if err == nil {
		key, _ := sdk.InvocationReceiptKey(ctx)
		wire, _ := json.Marshal(result)
		p := searchProbe{Kind: "result", Run: req.RunID, Key: key, Size: len(wire)}
		if result.Usage != nil && result.Usage.ScannedBytes != nil {
			p.Bytes = *result.Usage.ScannedBytes
		}
		appendProbe(t.fault.dir, p)
	}
	return result, err
}

type auditedReceipt struct {
	*durable.ReceiptTool
	fault searchFault
}

func (t auditedReceipt) Invoke(ctx context.Context, req sdk.ToolRequest) (sdk.ToolResult, error) {
	t.fault.record("invoke", ctx, req.RunID)
	result, err := t.ReceiptTool.Invoke(ctx, req)
	if err == nil && t.fault.mode == "hold-receipt" {
		t.fault.park("receipt-ready", req.RunID)
	}
	return result, err
}

func (t auditedReceipt) QueryReceipt(ctx context.Context, key sdk.ReceiptKey) (*sdk.ToolReceipt, error) {
	appendProbe(t.fault.dir, searchProbe{Kind: "query", Run: key.RunID, Key: key})
	return t.ReceiptTool.QueryReceipt(ctx, key)
}

type settledSearchModel struct {
	sdk.ModelProvider
	fault searchFault
}

func (m settledSearchModel) ContextLimits() sdk.ModelContextLimits {
	return sdk.ModelContextLimits{WindowTokens: 8192}
}

func (m settledSearchModel) MeasureInput(_ context.Context, req sdk.ModelRequest) (sdk.InputTokenMeasurement, error) {
	if req.Step == 2 {
		m.fault.park("settled", req.RunID)
	}
	return sdk.InputTokenMeasurement{Tokens: 24, Kind: sdk.MeasurementExact, ProfileVersion: "fault-fixture", InputDigest: "fixture"}, nil
}

func (m settledSearchModel) Generate(ctx context.Context, req sdk.ModelRequest) (sdk.ModelStream, error) {
	// Injection pauses before the runner writes the next model intent. The model
	// request itself still uses the command's production Messages adapter.
	req.Context = nil
	return m.ModelProvider.Generate(ctx, req)
}

func TestSearchCommandChild(t *testing.T) {
	mode := os.Getenv("SUPERAGENT_SEARCH_TEST")
	if mode == "" {
		return
	}
	offset := 1
	for offset < len(os.Args) && os.Args[offset] != "--" {
		offset++
	}
	if offset == len(os.Args) {
		t.Fatal("missing command arguments")
	}
	fault := searchFault{dir: os.Getenv("SUPERAGENT_SEARCH_AUDIT"), mode: mode}
	err := runCommand(t.Context(), os.Args[offset+1:], func(ctx context.Context, assembly terminalAssembly) error {
		binding := assembly.mesh.LocalTools["search"]
		journal, ok := binding.Tool.(*durable.ReceiptTool)
		if !ok {
			return errors.New("expected production receipt journal in search assembly")
		}
		journal.Tool = meteredSearch{Tool: journal.Tool, fault: fault}
		binding.Tool = auditedReceipt{ReceiptTool: journal, fault: fault}
		assembly.mesh.LocalTools["search"] = binding
		if mode == "hold-settled" {
			runner := assembly.mesh.LocalRunners["local"]
			runner.ContextPolicies = map[sdk.ModelRef]sdk.ModelContextPolicy{"chat": {MaxOutputTokens: 4096}}
			assembly.mesh.LocalRunners["local"] = runner
			assembly.mesh.LocalModels["chat"] = settledSearchModel{assembly.mesh.LocalModels["chat"], fault}
		}
		return launch(ctx, assembly)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (s *searchCase) probes(kind string, id sdk.RunID) []searchProbe {
	s.t.Helper()
	data, err := os.ReadFile(filepath.Join(s.root, "audit"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	must(s.t, err)
	var events []searchProbe
	for _, line := range bytes.Split(data[:bytes.LastIndexByte(data, '\n')+1], []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var event searchProbe
		must(s.t, json.Unmarshal(line, &event))
		if event.Kind == kind && (id == "" || event.Run == id) {
			events = append(events, event)
		}
	}
	return events
}

func (s *searchCase) awaitProbe(kind string) searchProbe {
	s.t.Helper()
	for deadline := time.Now().Add(7 * time.Second); time.Now().Before(deadline); {
		if found := s.probes(kind, ""); len(found) != 0 {
			return found[len(found)-1]
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.t.Fatalf("missing subprocess event %q", kind)
	return searchProbe{}
}

func (s *searchCase) probeCount(kind string, id sdk.RunID, want int) {
	s.t.Helper()
	if found := s.probes(kind, id); len(found) != want {
		s.t.Fatalf("%s %s got %d want %d: %+v", kind, id, len(found), want, found)
	}
}
