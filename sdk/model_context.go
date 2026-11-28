package sdk

import (
	"context"

	"github.com/shubolab/superagent/internal/contextview"
	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/mesh"
)

// InputDigest binds serialized supplier input to the model and measurement profile.
func InputDigest(modelID, profileVersion string, encoded []byte) string {
	return contract.InputDigest(modelID, profileVersion, encoded)
}

// VerifyInputDigest checks measured input immediately before sending the supplier request.
func VerifyInputDigest(opts *contract.ModelContextOptions, modelID, profileVersion string, encoded []byte) error {
	return contract.VerifyInputDigest(opts, modelID, profileVersion, encoded)
}

// NewContextSourceTool is an opt-in, run-scoped original-context reader.
func NewContextSourceTool() contract.LocalToolBinding {
	return contextview.NewSourceTool()
}

// ReadSource reads one bounded page of original context from this Run.
func (r *Run) ReadSource(ctx context.Context, ref contract.ContextSourceRef, offset, maxBytes uint64) (contract.ContextSourcePage, error) {
	req := r.request(mesh.ReadContextSource)
	req.Source = mesh.ContextSourceRequest{Ref: ref, Offset: offset, MaxBytes: maxBytes}
	reply, err := r.client.transport.Call(ctx, req)
	return reply.SourcePage, err
}
