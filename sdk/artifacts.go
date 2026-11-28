package sdk

import (
	"context"
	"errors"

	"github.com/shubolab/superagent/internal/artifact"
	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/mesh"
)

type LocalArtifactConfig = artifact.Config

// NewLocalArtifactStore creates a private store owned by the host assembly.
func NewLocalArtifactStore(config LocalArtifactConfig) (contract.ArtifactStore, error) {
	return artifact.NewLocal(config)
}

// NewLocalRetainedArtifactStore reopens the same private artifact directory across processes.
func NewLocalRetainedArtifactStore(config LocalArtifactConfig) (contract.ArtifactStore, error) {
	return artifact.NewRetained(config)
}

// OpenToolArtifact creates a writer scoped to the active call's artifact budget.
func OpenToolArtifact(ctx context.Context, req contract.ToolRequest) (contract.ArtifactWriter, error) {
	return contract.OpenToolArtifact(ctx, req)
}

// ReadArtifact reads a retained page owned by this Run and its authenticated caller.
func (r *Run) ReadArtifact(ctx context.Context, ref contract.ArtifactRef, offset, maxBytes uint64) (contract.ArtifactPage, error) {
	request := r.request(mesh.ReadArtifact)
	request.Artifact = mesh.ArtifactRead{Ref: ref, Offset: offset, MaxBytes: maxBytes}
	reply, err := r.client.transport.Call(ctx, request)
	if errors.Is(err, contract.ErrAccessDenied) {
		return contract.ArtifactPage{}, contract.ErrArtifactUnavailable
	}
	return reply.ArtifactPage, err
}
