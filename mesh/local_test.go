package mesh

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/shubolab/superagent/internal/contract"
)

type endpointStub struct {
	started contract.SubjectID
	stream  *streamStub[contract.Event]
}

func (e *endpointStub) Start(_ context.Context, caller contract.SubjectID, req contract.StartRequest) (contract.RunID, error) {
	e.started = caller
	return req.ID, nil
}

func (e *endpointStub) Wait(_ context.Context, _ contract.SubjectID, id contract.RunID) (contract.Result, error) {
	return contract.Result{RunID: id, Outcome: contract.Succeeded}, nil
}

func (e *endpointStub) Cancel(context.Context, contract.SubjectID, contract.RunID) error {
	return nil
}

func (e *endpointStub) Observe(context.Context, contract.SubjectID, contract.RunID) (contract.EventStream, error) {
	return e.stream, nil
}

type providerFunc func(context.Context, contract.ModelRequest) (contract.ModelStream, error)

func (f providerFunc) Generate(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
	return f(ctx, req)
}

type streamStub[T any] struct {
	frames []T
	closed bool
}

func (s *streamStub[T]) Recv() (T, error) {
	if len(s.frames) > 0 {
		frame := s.frames[0]
		s.frames = s.frames[1:]
		return frame, nil
	}
	var zero T
	return zero, io.EOF
}

func (s *streamStub[T]) Close() error { s.closed = true; return nil }

func TestLocalRoutesCallsAndStreams(t *testing.T) {
	ctx := t.Context()
	endpoint := &endpointStub{stream: &streamStub[contract.Event]{frames: []contract.Event{{Seq: 1}, {Seq: 2}}}}
	model := &streamStub[contract.ModelEvent]{frames: []contract.ModelEvent{{TextDelta: "a"}, {TextDelta: "b"}}}
	models := map[contract.ModelRef]contract.ModelProvider{"chat": providerFunc(func(ctx context.Context, req contract.ModelRequest) (contract.ModelStream, error) {
		if ctx.Value(modelSubjectKey{}) != contract.SubjectID("test") || req.RunID != "id" {
			t.Fatal("model route lost caller or Run ID")
		}
		return model, nil
	})}
	runners := map[contract.RunnerRef]RunnerEndpoint{"main": endpoint}
	local, err := NewLocal(runners, models, func(context.Context) (contract.SubjectID, error) { return "test", nil })
	if err != nil {
		t.Fatal(err)
	}
	delete(models, "chat")
	delete(runners, "main")
	reply, err := local.Call(ctx, MeshRequest{Operation: Start, Runner: "main", Start: contract.StartRequest{ID: "id"}})
	if err != nil || reply.RunID != "id" || endpoint.started != "test" {
		t.Fatalf("reply %+v, caller %q, error %v", reply, endpoint.started, err)
	}
	reply, err = local.Call(ctx, MeshRequest{Operation: Wait, Runner: "main", RunID: "id"})
	if err != nil || reply.Result.RunID != "id" {
		t.Fatalf("wait reply %+v, error %v", reply, err)
	}
	if _, err := local.Call(ctx, MeshRequest{Operation: Cancel, Runner: "missing"}); !errors.Is(err, contract.ErrBindingNotFound) {
		t.Fatalf("unknown runner error = %v", err)
	}
	stream, err := local.OpenStream(ctx, MeshRequest{Operation: Observe, Runner: "main", RunID: "id"})
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint64{1, 2} {
		event, err := Events(stream).Recv()
		if err != nil || event.Seq != seq {
			t.Fatalf("observation %+v, error %v", event, err)
		}
	}
	if err := stream.Close(); err != nil || !endpoint.stream.closed {
		t.Fatalf("observation close did not reach its source: %v", err)
	}
	modelStream, err := OpenModel(ctx, local, "test", contract.ModelRequest{Model: "chat", RunID: "id"})
	if err != nil {
		t.Fatal(err)
	}
	for _, delta := range []string{"a", "b"} {
		event, err := modelStream.Recv()
		if err != nil || event.TextDelta != delta {
			t.Fatalf("model event %+v, error %v", event, err)
		}
	}
	if _, err := modelStream.Recv(); err != io.EOF {
		t.Fatalf("model stream error = %v, want EOF", err)
	}
	if err := modelStream.Close(); err != nil || !model.closed {
		t.Fatalf("model close did not reach its source: %v", err)
	}
}
