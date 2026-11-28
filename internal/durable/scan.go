package durable

import (
	"context"
	"sync/atomic"

	"github.com/shubolab/superagent/internal/contract"
)

// ScanMeasurement accompanies a receipt only when the original environment closed its grant.
type ScanMeasurement struct {
	Limit uint64
	Bytes uint64
}

func (m *ScanMeasurement) VerifiedBytes(limit uint64, result contract.ToolResult) *uint64 {
	if m == nil || limit == 0 || m.Limit != limit || m.Bytes > limit ||
		result.Usage == nil || result.Usage.ScannedBytes == nil || *result.Usage.ScannedBytes != m.Bytes {
		return nil
	}
	bytes := m.Bytes
	return &bytes
}

type scanCaptureKey struct{}

type scanCapture struct {
	key      contract.ReceiptKey
	limit    uint64
	measure  func() (uint64, error)
	measured atomic.Pointer[ScanMeasurement]
}

// WithReceiptScan associates the trusted environment's meter or a read-only receipt lookup
// with the original identity. ToolResult usage alone cannot populate this capture.
func WithReceiptScan(ctx context.Context, key contract.ReceiptKey, limit uint64, measure func() (uint64, error)) (context.Context, *scanCapture) {
	scan := &scanCapture{key: key, limit: limit, measure: measure}
	return context.WithValue(ctx, scanCaptureKey{}, scan), scan
}

func receiptScanCapture(ctx context.Context, key contract.ReceiptKey) *scanCapture {
	scan, _ := ctx.Value(scanCaptureKey{}).(*scanCapture)
	if scan == nil || scan.key != key || scan.limit == 0 {
		return nil
	}
	return scan
}

func (s *scanCapture) report(measurement *ScanMeasurement) {
	if s != nil {
		s.measured.Store(measurement)
	}
}

func (s *scanCapture) Measurement(result contract.ToolResult) *ScanMeasurement {
	m := s.measured.Load()
	if m.VerifiedBytes(s.limit, result) == nil {
		return nil
	}
	return m
}
