package remotetool

import (
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

const (
	invokePath = "/mesh/v1/tool/invoke"
	queryPath  = "/mesh/v1/tool/receipt"

	maxRequestBytes = 1 << 20
	maxReceiptBytes = 1 << 20
	envelopeBytes   = 16 << 10
)

type request struct {
	ID        string
	Key       contract.ReceiptKey
	Scope     contract.RunScope
	Owner     contract.RemoteOwner
	Deadline  time.Time
	Tool      *contract.ToolRequest `json:",omitempty"`
	Limits    contract.ToolLimits
	Workspace bool `json:",omitempty"`
}

type response struct {
	ID       string
	Identity contract.RemoteToolIdentity
	Receipt  *contract.ToolReceipt    `json:",omitempty"`
	Scan     *durable.ScanMeasurement `json:",omitempty"`
	Ready    bool                     `json:",omitempty"`
}

func readMessage(src io.Reader, limit int64, value any) error {
	data, err := io.ReadAll(io.LimitReader(src, limit+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return errors.New("remote tool message exceeds limit")
	}
	return json.Unmarshal(data, value)
}

func validIdentity(id contract.RemoteToolIdentity) bool {
	return id.Provider != "" && id.Journal != "" && id.Version != "" &&
		len(id.Provider) <= 128 && len(id.Journal) <= 128 && len(id.Version) <= 128 &&
		len(id.Workspace) <= 128 && len(id.WorkspaceVersion) <= 128 &&
		(id.Workspace == "") == (id.WorkspaceVersion == "") &&
		(id.EvidenceRule == "" || (id.EvidenceRule == contract.AppendFileEvidenceV1 && id.Workspace != ""))
}

func validReceipt(receipt *contract.ToolReceipt, key contract.ReceiptKey, limit uint64) bool {
	if receipt == nil || receipt.Key != key || !contract.ValidToolResult(receipt.Result, key.RunID, key.CallID) {
		return false
	}
	if receipt.Cleanup != "" && receipt.Cleanup != contract.CleanupReleased && receipt.Cleanup != contract.CleanupPending {
		return false
	}
	result := receipt.Result
	if result.State != contract.ToolExecutionKnown && result.State != contract.ToolExecutionNotExecuted {
		return false
	}
	if result.Completion != nil && (len(result.Completion.Artifacts) != 0 || uint64(len(result.Completion.Content)) > min(maxReceiptBytes, limit)) {
		return false
	}
	if ref := receipt.Evidence; ref != nil {
		if key.Remote.EvidenceRule != contract.AppendFileEvidenceV1 ||
			result.State != contract.ToolExecutionKnown || result.Completion.Outcome != contract.ToolSucceeded ||
			ref.ID != durable.WriteEvidenceID(key) || !contract.ValidContentHash(ref.Version) {
			return false
		}
		var output contract.AppendFileResult
		if json.Unmarshal([]byte(result.Completion.Content), &output) != nil || !contract.ValidManagedPath(output.Path) ||
			output.Version != ref.Version || output.SHA256 != ref.Version || output.Bytes == 0 {
			return false
		}
	}
	encoded, err := json.Marshal(result)
	return err == nil && uint64(len(encoded)) <= min(maxReceiptBytes, limit)
}
