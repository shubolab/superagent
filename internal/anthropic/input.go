package anthropic

import (
	"bytes"
	"context"
	"strings"

	"github.com/shubolab/superagent/internal/contract"
)

// InputProfile describes a host-validated local meter for a single provider model.
type InputProfile struct {
	Limits      contract.ModelContextLimits
	Kind        contract.MeasurementKind
	Version     string
	CountTokens func(context.Context, []byte) (uint64, error)
}

func (p InputProfile) valid() bool {
	return p.Limits.WindowTokens > 0 && strings.TrimSpace(p.Version) != "" && p.CountTokens != nil &&
		(p.Kind == contract.MeasurementExact || p.Kind == contract.MeasurementEstimated)
}

type inputMeter struct {
	client  *Client
	modelID string
	profile InputProfile
}

func (c *Client) InputMeter(modelID string) contract.ModelInputMeter {
	profile, ok := c.profiles[modelID]
	if !ok {
		return nil
	}
	return inputMeter{client: c, modelID: modelID, profile: profile}
}

func (m inputMeter) ContextLimits() contract.ModelContextLimits { return m.profile.Limits }

func (m inputMeter) MeasureInput(ctx context.Context, req contract.ModelRequest) (contract.InputTokenMeasurement, error) {
	if err := ctx.Err(); err != nil {
		return contract.InputTokenMeasurement{}, err
	}
	body, err := m.client.encode(m.modelID, req)
	if err != nil {
		return contract.InputTokenMeasurement{}, err
	}
	tokens, err := m.profile.CountTokens(ctx, bytes.Clone(body))
	if err != nil {
		return contract.InputTokenMeasurement{}, contract.ErrContextMeasurementUnavailable
	}
	return contract.InputTokenMeasurement{
		Tokens: tokens, Kind: m.profile.Kind, ProfileVersion: m.profile.Version,
		InputDigest: contract.InputDigest(m.modelID, m.profile.Version, body),
	}, nil
}

func (c *Client) encode(modelID string, req contract.ModelRequest) ([]byte, error) {
	maxOutput := c.tokens
	if opts := req.Context; opts != nil {
		if _, ok := c.profiles[modelID]; !ok {
			return nil, contract.ErrContextMeasurementUnavailable
		}
		if opts.MaxOutputTokens == 0 || opts.MaxOutputTokens > c.tokens {
			return nil, contract.ErrContextInputChanged
		}
		maxOutput = opts.MaxOutputTokens
	}
	return requestBody(modelID, maxOutput, req)
}
