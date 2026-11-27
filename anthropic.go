package superagent

import (
	"context"

	"github.com/shubolab/superagent/internal/anthropic"
)

// NewAnthropicMessagesAdapter creates a factory adapter for text and client-side tools.
// maxOutputTokens caps a provider response independently of the SDK's Run byte limits.
func NewAnthropicMessagesAdapter(maxOutputTokens uint64) ModelAdapter {
	return ModelAdapter{
		DefaultEndpoint: anthropic.DefaultEndpoint, RequiresCredential: true,
		Open: func(_ context.Context, connection ProviderConnection) (ModelAdapterClient, error) {
			return anthropic.New(connection.Endpoint, connection.Credential, maxOutputTokens)
		},
	}
}
