package superagent_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shubolab/superagent"
)

type modelFunc func(context.Context, superagent.ModelRequest) (superagent.ModelStream, error)

func (f modelFunc) Generate(ctx context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
	return f(ctx, req)
}

var _ superagent.ModelProvider = modelFunc(nil)

type callerKey struct{}

func hostContext(t *testing.T, subject superagent.SubjectID) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), callerKey{}, subject), 8*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func pointer[T any](v T) *T { return &v }

func hostConfig(model superagent.ModelProvider) superagent.MeshConfig {
	budget := superagent.Budget{
		Timeout: pointer(5 * time.Second), MaxModelRequests: pointer(uint64(1)),
		MaxModelInputBytes: pointer(uint64(64 * 1024)), MaxOutputBytes: pointer(uint64(1024)),
	}
	return superagent.MeshConfig{
		LocalModels: map[superagent.ModelRef]superagent.ModelProvider{"chat": model},
		LocalRunners: map[superagent.RunnerRef]superagent.RunnerConfig{"main": {
			AllowedModels: map[superagent.SubjectID][]superagent.ModelRef{"alice": {"chat"}, "bob": {"chat"}},
			DefaultBudget: budget, BudgetCaps: budget, ObserveBufferBytes: 16 * 1024, ResultRetention: time.Minute,
		}},
		ResolveCaller: func(ctx context.Context) (superagent.SubjectID, error) {
			subject, _ := ctx.Value(callerKey{}).(superagent.SubjectID)
			return subject, nil
		},
	}
}

func newClient(t *testing.T, cfg superagent.MeshConfig) *superagent.Client {
	t.Helper()
	mesh, err := superagent.NewMesh(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := superagent.NewClient(mesh, "main")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func request(id superagent.RunID) superagent.StartRequest {
	return superagent.StartRequest{ID: id, Model: "chat", Input: superagent.UserMessage{Text: "greet"}}
}

func start(t *testing.T, ctx context.Context, c *superagent.Client, req superagent.StartRequest) *superagent.Run {
	t.Helper()
	run, err := c.Start(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if run.ID() != req.ID {
		t.Fatalf("Run ID = %q, want %q", run.ID(), req.ID)
	}
	return run
}

func wait(t *testing.T, ctx context.Context, r *superagent.Run, want superagent.RunOutcome) superagent.Result {
	t.Helper()
	result, err := r.Wait(ctx)
	if err != nil || result.Outcome != want || result.RunID != r.ID() {
		t.Fatalf("result = %+v, error = %v, want %s for %s", result, err, want, r.ID())
	}
	return result
}

func recv[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}

type controlledStream struct {
	events    chan superagent.ModelEvent
	receiving chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
	closeHits atomic.Int32
	late      *superagent.ModelEvent
}

func scripted(events ...superagent.ModelEvent) *controlledStream {
	s := &controlledStream{
		events: make(chan superagent.ModelEvent, len(events)+16), receiving: make(chan struct{}, 32), closed: make(chan struct{}),
	}
	for _, event := range events {
		s.events <- event
	}
	return s
}

func (s *controlledStream) Recv() (superagent.ModelEvent, error) {
	select {
	case s.receiving <- struct{}{}:
	default:
	}
	// The fixture needs Close to release an idle Recv.
	select {
	case event, ok := <-s.events:
		if ok {
			return event, nil
		}
		return superagent.ModelEvent{}, io.EOF
	case <-s.closed:
		if s.late != nil {
			return *s.late, nil
		}
		return superagent.ModelEvent{}, superagent.ErrStreamClosed
	}
}

func (s *controlledStream) Close() error {
	s.closeHits.Add(1)
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

type modelCall struct {
	ctx context.Context
	req superagent.ModelRequest
}

type probe struct {
	calls   chan modelCall
	count   atomic.Int32
	streams map[superagent.RunID]*controlledStream
}

func newProbe(streams map[superagent.RunID]*controlledStream) *probe {
	return &probe{calls: make(chan modelCall, 64), streams: streams}
}

func (m *probe) Generate(ctx context.Context, req superagent.ModelRequest) (superagent.ModelStream, error) {
	m.count.Add(1)
	m.calls <- modelCall{ctx, req}
	s := m.streams[req.RunID]
	if s == nil {
		return nil, errors.New("unexpected run")
	}
	return s, nil
}

func delta(text string) superagent.ModelEvent {
	return superagent.ModelEvent{Kind: superagent.ModelTextDelta, TextDelta: text}
}

func completed(text string) superagent.ModelEvent {
	return superagent.ModelEvent{
		Kind:  superagent.ModelCompleted,
		Reply: &superagent.ModelReply{Finish: superagent.ModelFinishStop, Text: text},
	}
}

func TestNewMeshAndClientCheckBindings(t *testing.T) {
	model := modelFunc(func(context.Context, superagent.ModelRequest) (superagent.ModelStream, error) {
		return nil, errors.New("unused")
	})
	for name, change := range map[string]func(*superagent.MeshConfig){
		"missing resolver": func(cfg *superagent.MeshConfig) { cfg.ResolveCaller = nil },
		"nil provider":     func(cfg *superagent.MeshConfig) { cfg.LocalModels["chat"] = modelFunc(nil) },
		"missing granted model": func(cfg *superagent.MeshConfig) {
			runner := cfg.LocalRunners["main"]
			runner.AllowedModels["alice"] = []superagent.ModelRef{"missing"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := hostConfig(model)
			change(&cfg)
			if _, err := superagent.NewMesh(cfg); !errors.Is(err, superagent.ErrInvalidConfiguration) {
				t.Fatalf("assembly error = %v", err)
			}
		})
	}
	mesh, err := superagent.NewMesh(hostConfig(model))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := superagent.NewClient(mesh, "missing"); !errors.Is(err, superagent.ErrBindingNotFound) {
		t.Fatalf("unknown runner error = %v", err)
	}
}
