package mesh

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"sync"

	"github.com/shubolab/superagent/internal/contract"
)

type RunnerEndpoint interface {
	Start(context.Context, contract.SubjectID, contract.StartRequest) (contract.RunID, error)
	Wait(context.Context, contract.SubjectID, contract.RunID) (contract.Result, error)
	Cancel(context.Context, contract.SubjectID, contract.RunID) error
	Observe(context.Context, contract.SubjectID, contract.RunID) (contract.EventStream, error)
}

type Local struct {
	invalidEstimates sync.Map
	runners          map[contract.RunnerRef]RunnerEndpoint
	models           map[contract.ModelRef]contract.ModelProvider
	resolveCaller    func(context.Context) (contract.SubjectID, error)
	tools            map[contract.ToolRef]contract.LocalToolBinding
	environments     map[contract.EnvironmentRef]contract.EnvironmentProvider
	artifacts        contract.ArtifactStore
}

func NewLocal(
	runners map[contract.RunnerRef]RunnerEndpoint,
	models map[contract.ModelRef]contract.ModelProvider,
	resolveCaller func(context.Context) (contract.SubjectID, error),
) (*Local, error) {
	if len(runners) == 0 || len(models) == 0 || resolveCaller == nil {
		return nil, fmt.Errorf("%w: runners, models and caller resolution are required", contract.ErrInvalidConfiguration)
	}
	for ref, endpoint := range runners {
		if ref == "" || nilBinding(endpoint) {
			return nil, fmt.Errorf("%w: invalid runner %q", contract.ErrInvalidConfiguration, ref)
		}
	}
	for ref, provider := range models {
		if ref == "" || nilBinding(provider) {
			return nil, fmt.Errorf("%w: invalid model %q", contract.ErrInvalidConfiguration, ref)
		}
	}
	return &Local{runners: maps.Clone(runners), models: maps.Clone(models), resolveCaller: resolveCaller}, nil
}

func (l *Local) HasRunner(ref contract.RunnerRef) bool {
	_, ok := l.runners[ref]
	return ok
}

func (l *Local) Call(ctx context.Context, req MeshRequest) (MeshReply, error) {
	if req.Operation == MeasureModelInput || req.Operation == InvalidateModelEstimate {
		return l.modelInputCall(ctx, req)
	}
	if req.Operation >= Invoke {
		return l.componentCall(ctx, req)
	}
	runner, subject, err := l.runnerCall(ctx, req.Runner)
	if err != nil {
		return MeshReply{}, err
	}
	switch req.Operation {
	case Start:
		id, err := runner.Start(ctx, subject, req.Start)
		return MeshReply{RunID: id}, err
	case Recover:
		recoverer, ok := runner.(interface {
			Recover(context.Context, contract.SubjectID, contract.RunID) (contract.RunID, error)
		})
		if !ok {
			return MeshReply{}, contract.ErrRecoveryUnavailable
		}
		id, err := recoverer.Recover(ctx, subject, req.RunID)
		return MeshReply{RunID: id}, err
	case Wait:
		result, err := runner.Wait(ctx, subject, req.RunID)
		return MeshReply{Result: result}, err
	case Cancel:
		return MeshReply{}, runner.Cancel(ctx, subject, req.RunID)
	case ReadArtifact:
		return l.readArtifact(ctx, subject, req)
	case ReadContextSource:
		reader, ok := runner.(interface {
			ReadSource(context.Context, contract.SubjectID, contract.RunID, ContextSourceRequest) (contract.ContextSourcePage, error)
		})
		if !ok {
			return MeshReply{}, contract.ErrContextSourceUnavailable
		}
		page, err := reader.ReadSource(ctx, subject, req.RunID, req.Source)
		return MeshReply{SourcePage: page}, err
	default:
		return MeshReply{}, contract.ErrInvalidRequest
	}
}

type modelSubjectKey struct{}

func (l *Local) OpenStream(ctx context.Context, req MeshRequest) (MeshStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch req.Operation {
	case Observe:
		runner, subject, err := l.runnerCall(ctx, req.Runner)
		if err != nil {
			return nil, err
		}
		stream, err := runner.Observe(ctx, subject, req.RunID)
		if err != nil {
			return nil, err
		}
		return mapFrames(stream, func(event contract.Event) MeshFrame { return MeshFrame{Event: event} }), nil
	case Generate:
		subject, _ := ctx.Value(modelSubjectKey{}).(contract.SubjectID)
		if subject == "" {
			return nil, contract.ErrAccessDenied
		}
		provider, ok := l.models[req.Model.Model]
		if !ok {
			return nil, contract.ErrBindingNotFound
		}
		if opts := req.Model.Context; opts != nil && l.estimateInvalid(req.Model.Model, opts.ProfileVersion) {
			return nil, contract.ErrContextMeasurementUnavailable
		}
		stream, err := provider.Generate(ctx, req.Model)
		if err != nil {
			return nil, err
		}
		if nilBinding(stream) {
			return nil, contract.ErrInvalidRequest
		}
		return mapFrames(stream, func(event contract.ModelEvent) MeshFrame { return MeshFrame{ModelEvent: event} }), nil
	default:
		return nil, contract.ErrInvalidRequest
	}
}

func (l *Local) runnerCall(ctx context.Context, ref contract.RunnerRef) (RunnerEndpoint, contract.SubjectID, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	runner, ok := l.runners[ref]
	if !ok {
		return nil, "", contract.ErrBindingNotFound
	}
	subject, err := l.resolveCaller(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, "", ctxErr
	}
	if err != nil || subject == "" {
		return nil, "", contract.ErrAccessDenied
	}
	return runner, subject, nil
}

// OpenModel carries the subject from the admitted Run across the model binding.
func OpenModel(ctx context.Context, transport MeshTransport, subject contract.SubjectID, req contract.ModelRequest) (contract.ModelStream, error) {
	ctx = context.WithValue(ctx, modelSubjectKey{}, subject)
	stream, err := transport.OpenStream(ctx, MeshRequest{Operation: Generate, Model: req})
	if err != nil {
		return nil, err
	}
	return mapFrames(stream, func(frame MeshFrame) contract.ModelEvent { return frame.ModelEvent }), nil
}

func Events(stream MeshStream) contract.EventStream {
	return mapFrames(stream, func(frame MeshFrame) contract.Event { return frame.Event })
}

type framedStream[T any] interface {
	Recv() (T, error)
	Close() error
}

type mappedStream[T, U any] struct {
	inner     framedStream[T]
	transform func(T) U
}

func mapFrames[T, U any](inner framedStream[T], transform func(T) U) mappedStream[T, U] {
	return mappedStream[T, U]{inner: inner, transform: transform}
}

func (s mappedStream[T, U]) Recv() (U, error) {
	value, err := s.inner.Recv()
	if err != nil {
		var zero U
		return zero, err
	}
	return s.transform(value), nil
}

func (s mappedStream[T, U]) Close() error { return s.inner.Close() }

func nilBinding(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
