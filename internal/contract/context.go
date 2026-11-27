package contract

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
)

type ModelInputMeter interface {
	ContextLimits() ModelContextLimits
	MeasureInput(context.Context, ModelRequest) (InputTokenMeasurement, error)
}

type ModelContextLimits struct {
	WindowTokens   uint64
	MaxInputTokens uint64
}

type MeasurementKind string

const (
	MeasurementExact     MeasurementKind = "exact"
	MeasurementEstimated MeasurementKind = "estimated"
)

type InputTokenMeasurement struct {
	Tokens         uint64
	Kind           MeasurementKind
	ProfileVersion string
	InputDigest    string
}

type ModelContextOptions struct {
	Summary             []ContextSummaryEntry
	MaxOutputTokens     uint64
	ExpectedInputDigest string
	ProfileVersion      string
}

type ContextSourceRef string

type ContextSummaryEntry struct {
	Section string
	Text    string
	Sources []ContextSourceRef
}

// ModelContextPolicy opts a model binding into window metering.
type ModelContextPolicy struct {
	MaxOutputTokens           uint64
	SafetyMarginTokens        uint64
	CalibratedEstimateVersion string
}

var (
	ErrContextMeasurementUnavailable = errors.New("context_measurement_unavailable")
	ErrContextInputChanged           = errors.New("context_input_changed")
	ErrModelContextOverflow          = errors.New("model_context_overflow")
)

// InputDigest binds a provider's exact serialized request to its model and accounting profile.
// Credentials and the expected digest are control data and must be left out of encoded.
func InputDigest(modelID, profileVersion string, encoded []byte) string {
	h := sha256.New()
	for _, field := range []string{modelID, profileVersion} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(field))
	}
	_, _ = h.Write(encoded)
	return hex.EncodeToString(h.Sum(nil))
}

// VerifyInputDigest is called after encoding and before sending a model request.
func VerifyInputDigest(opts *ModelContextOptions, modelID, profileVersion string, encoded []byte) error {
	if opts == nil {
		return nil
	}
	if opts.ExpectedInputDigest == "" || opts.ProfileVersion != profileVersion ||
		opts.ExpectedInputDigest != InputDigest(modelID, profileVersion, encoded) {
		return ErrContextInputChanged
	}
	return nil
}
