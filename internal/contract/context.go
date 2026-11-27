package contract

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"time"
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
	Compaction                *ContextCompactionPolicy
}

type ContextCompactionPolicy struct {
	TriggerRatio         float64
	TargetRatio          float64
	KeepRecentTokens     uint64
	SummaryMaxTokens     uint64
	SummaryMaxBytes      uint64
	MinSavedTokens       uint64
	MaxCompactionsPerRun uint64
	SourcePageBytes      uint64
	SourceTool           ToolRef
}

type ContextCompactionDiagnostic struct {
	Step                uint32
	Trigger             string
	Selection           string
	BeforeTokens        uint64
	AfterTokens         uint64
	BeforeBytes         uint64
	AfterBytes          uint64
	HasCoverage         bool
	FirstCoveredMessage int
	LastCoveredMessage  int
	SummaryInputBytes   uint64
	SummaryOutputBytes  uint64
	Elapsed             time.Duration
}

type ContextSourceReader interface {
	ReadSource(context.Context, ContextSourceRef, uint64, uint64) (ContextSourcePage, error)
}

type ContextSourcePage struct {
	Content    []byte
	NextOffset uint64
	EOF        bool
}

type contextSourceKey struct{}

func WithContextSource(ctx context.Context, reader ContextSourceReader) context.Context {
	return context.WithValue(ctx, contextSourceKey{}, reader)
}

func ContextSource(ctx context.Context) ContextSourceReader {
	reader, _ := ctx.Value(contextSourceKey{}).(ContextSourceReader)
	return reader
}

var (
	ErrContextMeasurementUnavailable = errors.New("context_measurement_unavailable")
	ErrContextSourceUnavailable      = errors.New("context_source_unavailable")
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
