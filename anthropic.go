package superagent

import (
	"context"

	"github.com/shubolab/superagent/model"
)

// NewAnthropicMessagesAdapter creates a factory adapter for text and client-side tools.
// maxOutputTokens caps a provider response independently of the SDK's Run byte limits.
func NewAnthropicMessagesAdapter(maxOutputTokens uint64) ModelAdapter {
	return NewAnthropicMessagesAdapterWithOptions(AnthropicMessagesOptions{MaxOutputTokens: maxOutputTokens})
}

// AnthropicInputProfile supplies the host-validated local meter for one ModelID.
type AnthropicInputProfile = model.AnthropicInputProfile

type AnthropicAuthentication = model.AnthropicAuthentication

const (
	AnthropicAuthAPIKey = model.AnthropicAuthAPIKey
	AnthropicAuthBearer = model.AnthropicAuthBearer
)

type AnthropicMessagesOptions = model.AnthropicMessagesOptions

// NewMeteredAnthropicMessagesAdapter registers an explicit local meter for each supplied ModelID.
func NewMeteredAnthropicMessagesAdapter(maxOutputTokens uint64, profiles map[string]AnthropicInputProfile) ModelAdapter {
	return NewAnthropicMessagesAdapterWithOptions(AnthropicMessagesOptions{MaxOutputTokens: maxOutputTokens, InputProfiles: profiles})
}

// NewAnthropicMessagesAdapterWithOptions configures a specific Anthropic-compatible binding.
// Enable DiscardThinking only on gateways that accept continuation without those blocks.
func NewAnthropicMessagesAdapterWithOptions(options AnthropicMessagesOptions) ModelAdapter {
	adapter := model.NewAnthropicMessagesAdapterWithOptions(options)
	return ModelAdapter{
		DefaultEndpoint: adapter.DefaultEndpoint, RequiresCredential: adapter.RequiresCredential,
		Open: func(ctx context.Context, connection ProviderConnection) (ModelAdapterClient, error) {
			return adapter.Open(ctx, model.Connection(connection))
		},
	}
}
