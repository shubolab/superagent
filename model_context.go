package superagent

import (
	"context"

	"github.com/shubolab/superagent/sdk"
)

// InputDigest binds serialized supplier input to the model and measurement profile.
func InputDigest(modelID, profileVersion string, encoded []byte) string {
	return sdk.InputDigest(modelID, profileVersion, encoded)
}

// VerifyInputDigest checks measured input immediately before sending the supplier request.
func VerifyInputDigest(opts *ModelContextOptions, modelID, profileVersion string, encoded []byte) error {
	return sdk.VerifyInputDigest(opts, modelID, profileVersion, encoded)
}

// NewContextSourceTool is an opt-in, run-scoped original-context reader.
func NewContextSourceTool() LocalToolBinding {
	return sdk.NewContextSourceTool()
}

// ReadSource reads one bounded page of original context from this Run.
func (r *Run) ReadSource(ctx context.Context, ref ContextSourceRef, offset, maxBytes uint64) (ContextSourcePage, error) {
	return (*sdk.Run)(r).ReadSource(ctx, ref, offset, maxBytes)
}
