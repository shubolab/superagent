package remotetool

import (
	"errors"
	"testing"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

func TestRenderAppendResultRequiresCompleteEvidenceWithinOriginalBudget(t *testing.T) {
	intent := contract.WriteIntent{Key: contract.ReceiptKey{Runner: "runner", Subject: "owner", RunID: "run",
		CallID: "c1", Remote: contract.RemoteToolIdentity{EvidenceRule: contract.AppendFileEvidenceV1}},
		Path: "notes.txt", AppendHash: contract.HashBytes([]byte("saved")), AppendBytes: 5,
		Limits: contract.ToolLimits{MaxResultBytes: 1024}}
	evidence := contract.WriteEvidence{Intent: intent,
		Version: contract.FileVersion{ID: intent.AppendHash, Bytes: 5},
		Ref:     contract.WriteEvidenceRef{ID: durable.WriteEvidenceID(intent.Key), Version: intent.AppendHash}}
	if result, err := renderAppendResult(intent, evidence); err != nil || result.CallID != intent.Key.CallID ||
		result.Completion.Outcome != contract.ToolSucceeded {
		t.Fatalf("complete proof: %+v (%v)", result, err)
	}
	t.Run("over original cap", func(t *testing.T) {
		small, proof := intent, evidence
		small.Limits.MaxResultBytes = 64
		proof.Intent = small
		if _, err := renderAppendResult(small, proof); !errors.Is(err, contract.ErrWriteEvidenceUnavailable) {
			t.Fatalf("oversized reconstruction: %v", err)
		}
	})
	t.Run("incomplete version", func(t *testing.T) {
		bad := evidence
		bad.Version.Bytes = 0
		if _, err := renderAppendResult(intent, bad); !errors.Is(err, contract.ErrWriteEvidenceUnavailable) {
			t.Fatalf("incomplete reconstruction: %v", err)
		}
	})
}
