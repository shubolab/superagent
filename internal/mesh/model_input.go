package mesh

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
)

type profileKey struct {
	model   contract.ModelRef
	version string
}

func (l *Local) estimateInvalid(ref contract.ModelRef, profile string) bool {
	_, invalid := l.invalidEstimates.Load(profileKey{model: ref, version: profile})
	return invalid
}

func (l *Local) modelInputCall(ctx context.Context, req MeshRequest) (MeshReply, error) {
	if err := ctx.Err(); err != nil {
		return MeshReply{}, err
	}
	subject, _ := ctx.Value(modelSubjectKey{}).(contract.SubjectID)
	if subject == "" {
		return MeshReply{}, contract.ErrAccessDenied
	}
	provider, ok := l.models[req.Model.Model]
	if !ok {
		return MeshReply{}, contract.ErrBindingNotFound
	}
	if req.Operation == InvalidateModelEstimate {
		if req.ProfileVersion == "" {
			return MeshReply{}, contract.ErrInvalidRequest
		}
		l.invalidEstimates.Store(profileKey{model: req.Model.Model, version: req.ProfileVersion}, struct{}{})
		return MeshReply{}, nil
	}
	meter, ok := provider.(contract.ModelInputMeter)
	if !ok {
		return MeshReply{}, contract.ErrContextMeasurementUnavailable
	}
	limits := meter.ContextLimits()
	measurement, err := meter.MeasureInput(ctx, req.Model)
	if err != nil {
		return MeshReply{}, err
	}
	if measurement.Kind == contract.MeasurementEstimated && l.estimateInvalid(req.Model.Model, measurement.ProfileVersion) {
		return MeshReply{}, contract.ErrContextMeasurementUnavailable
	}
	return MeshReply{ModelLimits: limits, Measurement: measurement}, nil
}

func MeasureInput(ctx context.Context, t MeshTransport, subject contract.SubjectID, req contract.ModelRequest) (MeshReply, error) {
	ctx = context.WithValue(ctx, modelSubjectKey{}, subject)
	return t.Call(ctx, MeshRequest{Operation: MeasureModelInput, Model: req})
}

func InvalidateEstimate(ctx context.Context, t MeshTransport, subject contract.SubjectID, model contract.ModelRef, version string) error {
	ctx = context.WithValue(ctx, modelSubjectKey{}, subject)
	_, err := t.Call(ctx, MeshRequest{
		Operation: InvalidateModelEstimate, Model: contract.ModelRequest{Model: model}, ProfileVersion: version,
	})
	return err
}
