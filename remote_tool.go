package superagent

import (
	"encoding/json"
	"net/http"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/remotetool"
	"github.com/shubolab/superagent/internal/run"
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
	return remotetool.NewClient(cfg)
}

// NewRemoteToolHandler serves one tool over HTTPS with verified client certificates.
func NewRemoteToolHandler(cfg RemoteToolServerConfig) (http.Handler, error) {
	return remotetool.NewHandler(cfg)
}

// NewSharedRemoteToolAuthority uses the same host's shared takeover checkpoint directory.
func NewSharedRemoteToolAuthority(directory string) (RemoteToolAuthority, error) {
	return run.NewRemoteAuthority(directory)
}

// PrepareEvidenceAppend validates the arguments for an opt-in append-file/v1 Mesh binding.
func PrepareEvidenceAppend(args json.RawMessage) (string, error) {
	path, _, err := contract.ParseEvidenceAppend(args)
	return path, err
}

const AppendFileEvidenceV1 = contract.AppendFileEvidenceV1
