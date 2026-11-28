package superagent

import (
	"context"

	"github.com/shubolab/superagent/internal/artifact"
	"github.com/shubolab/superagent/sdk"
)

type LocalArtifactConfig = artifact.Config

// NewLocalArtifactStore creates a private store owned by the host assembly.
func NewLocalArtifactStore(config LocalArtifactConfig) (ArtifactStore, error) {
	return sdk.NewLocalArtifactStore(config)
}

// NewLocalRetainedArtifactStore reopens the same private artifact directory across processes.
func NewLocalRetainedArtifactStore(config LocalArtifactConfig) (ArtifactStore, error) {
	return sdk.NewLocalRetainedArtifactStore(config)
}

// OpenToolArtifact creates a writer scoped to the active call's artifact budget.
func OpenToolArtifact(ctx context.Context, req ToolRequest) (ArtifactWriter, error) {
	return sdk.OpenToolArtifact(ctx, req)
}

// ReadArtifact reads a retained page owned by this Run and its authenticated caller.
func (r *Run) ReadArtifact(ctx context.Context, ref ArtifactRef, offset, maxBytes uint64) (ArtifactPage, error) {
	return (*sdk.Run)(r).ReadArtifact(ctx, ref, offset, maxBytes)
}
