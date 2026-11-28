package superagent

import (
	"encoding/json"
	"net/http"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/remotetool"
	"github.com/shubolab/superagent/sdk"
)

type (
	RemoteToolIdentity     = contract.RemoteToolIdentity
	RemoteToolClientConfig = remotetool.ClientConfig
	RemoteToolServerConfig = remotetool.ServerConfig
	RemoteToolAccess       = remotetool.Access
	RemoteToolAuthority    = remotetool.Authority
	RemoteOwner            = contract.RemoteOwner
)

// NewRemoteTool creates a receipt-capable tool binding for registration in Mesh.
func NewRemoteTool(cfg RemoteToolClientConfig) (Tool, error) {
	return sdk.NewRemoteTool(cfg)
}

// NewRemoteToolHandler serves one tool over HTTPS with verified client certificates.
// Workspace writes require AppendFileEvidenceV1 and an EvidenceWorkspace.
func NewRemoteToolHandler(cfg RemoteToolServerConfig) (http.Handler, error) {
	return sdk.NewRemoteToolHandler(cfg)
}

// NewSharedRemoteToolAuthority uses the same host's shared takeover checkpoint directory.
func NewSharedRemoteToolAuthority(directory string) (RemoteToolAuthority, error) {
	return sdk.NewSharedRemoteToolAuthority(directory)
}

// PrepareEvidenceAppend validates arguments for an append-file/v1 Mesh binding.
func PrepareEvidenceAppend(args json.RawMessage) (string, error) {
	return sdk.PrepareEvidenceAppend(args)
}

const AppendFileEvidenceV1 = contract.AppendFileEvidenceV1
