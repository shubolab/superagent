package contract

import legacy "github.com/shubolab/superagent/internal/contract"

type (
	ModelInputMeter             = legacy.ModelInputMeter
	ModelContextLimits          = legacy.ModelContextLimits
	MeasurementKind             = legacy.MeasurementKind
	InputTokenMeasurement       = legacy.InputTokenMeasurement
	ModelContextOptions         = legacy.ModelContextOptions
	ContextSourceRef            = legacy.ContextSourceRef
	ContextSummaryEntry         = legacy.ContextSummaryEntry
	ModelContextPolicy          = legacy.ModelContextPolicy
	ContextCompactionPolicy     = legacy.ContextCompactionPolicy
	ContextCompactionDiagnostic = legacy.ContextCompactionDiagnostic
	ContextSourceReader         = legacy.ContextSourceReader
	ContextSourcePage           = legacy.ContextSourcePage
)

const (
	MeasurementExact     = legacy.MeasurementExact
	MeasurementEstimated = legacy.MeasurementEstimated
)

var (
	ErrContextMeasurementUnavailable = legacy.ErrContextMeasurementUnavailable
	ErrContextSourceUnavailable      = legacy.ErrContextSourceUnavailable
	ErrContextInputChanged           = legacy.ErrContextInputChanged
	ErrModelContextOverflow          = legacy.ErrModelContextOverflow
)
