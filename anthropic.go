package superagent

import (
	"context"
	"maps"

	"github.com/shubolab/superagent/internal/anthropic"
)

// NewAnthropicMessagesAdapter creates a factory adapter for text and client-side tools.
// maxOutputTokens caps a provider response independently of the SDK's Run byte limits.
func NewAnthropicMessagesAdapter(maxOutputTokens uint64) ModelAdapter {
	return NewMeteredAnthropicMessagesAdapter(maxOutputTokens, nil)
}

// AnthropicInputProfile supplies the host-validated local meter for one ModelID.
type AnthropicInputProfile = anthropic.InputProfile

// NewMeteredAnthropicMessagesAdapter registers an explicit local meter for each supplied ModelID.
func NewMeteredAnthropicMessagesAdapter(maxOutputTokens uint64, profiles map[string]AnthropicInputProfile) ModelAdapter {
	profiles = maps.Clone(profiles)
	return ModelAdapter{
		DefaultEndpoint: anthropic.DefaultEndpoint, RequiresCredential: true,
		Open: func(_ context.Context, connection ProviderConnection) (ModelAdapterClient, error) {
			return anthropic.New(connection.Endpoint, connection.Credential, maxOutputTokens, profiles)
		},
	}
}
