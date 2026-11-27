package superagent

import "github.com/shubolab/superagent/internal/contract"

// InputDigest binds serialized supplier input to the model and measurement profile.
func InputDigest(modelID, profileVersion string, encoded []byte) string {
	return contract.InputDigest(modelID, profileVersion, encoded)
}

// VerifyInputDigest checks measured input immediately before sending the supplier request.
func VerifyInputDigest(opts *ModelContextOptions, modelID, profileVersion string, encoded []byte) error {
	return contract.VerifyInputDigest(opts, modelID, profileVersion, encoded)
}
