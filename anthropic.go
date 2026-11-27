package superagent

import (
	"context"
	"maps"

	"github.com/shubolab/superagent/internal/anthropic"
)

// NewAnthropicMessagesAdapter creates a factory adapter for text and client-side tools.
// maxOutputTokens caps a provider response independently of the SDK's Run byte limits.
func NewAnthropicMessagesAdapter(maxOutputTokens uint64) ModelAdapter {
	return NewAnthropicMessagesAdapterWithOptions(AnthropicMessagesOptions{MaxOutputTokens: maxOutputTokens})
}

// AnthropicInputProfile supplies the host-validated local meter for one ModelID.
type AnthropicInputProfile = anthropic.InputProfile

type AnthropicAuthentication = anthropic.Authentication

const (
	AnthropicAuthAPIKey = anthropic.AuthAPIKey
	AnthropicAuthBearer = anthropic.AuthBearer
)

type AnthropicMessagesOptions = anthropic.Options

// NewMeteredAnthropicMessagesAdapter registers an explicit local meter for each supplied ModelID.
func NewMeteredAnthropicMessagesAdapter(maxOutputTokens uint64, profiles map[string]AnthropicInputProfile) ModelAdapter {
	return NewAnthropicMessagesAdapterWithOptions(AnthropicMessagesOptions{MaxOutputTokens: maxOutputTokens, InputProfiles: profiles})
}

// NewAnthropicMessagesAdapterWithOptions configures a specific Anthropic-compatible binding.
// Enable DiscardThinking only on gateways that accept continuation without those blocks.
func NewAnthropicMessagesAdapterWithOptions(options AnthropicMessagesOptions) ModelAdapter {
	options.InputProfiles = maps.Clone(options.InputProfiles)
	return ModelAdapter{
		DefaultEndpoint: anthropic.DefaultEndpoint, RequiresCredential: true,
		Open: func(_ context.Context, connection ProviderConnection) (ModelAdapterClient, error) {
			return anthropic.New(connection.Endpoint, connection.Credential, options)
		},
	}
}
