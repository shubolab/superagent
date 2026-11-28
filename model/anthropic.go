package model

import (
	"context"
	"maps"

	"github.com/shubolab/superagent/internal/anthropic"
)

type AnthropicInputProfile = anthropic.InputProfile
type AnthropicAuthentication = anthropic.Authentication

const (
	AnthropicAuthAPIKey = anthropic.AuthAPIKey
	AnthropicAuthBearer = anthropic.AuthBearer
)

type AnthropicMessagesOptions = anthropic.Options

// NewAnthropicMessagesAdapterWithOptions configures an Anthropic-compatible adapter.
func NewAnthropicMessagesAdapterWithOptions(options AnthropicMessagesOptions) Adapter {
	options.InputProfiles = maps.Clone(options.InputProfiles)
	return Adapter{
		DefaultEndpoint: anthropic.DefaultEndpoint, RequiresCredential: true,
		Open: func(_ context.Context, connection Connection) (AdapterClient, error) {
			return anthropic.New(connection.Endpoint, connection.Credential, options)
		},
	}
}
