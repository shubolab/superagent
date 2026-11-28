// Package session defines the persistence contract for SDK sessions.
package session

import (
	"context"
	"errors"

	"github.com/shubolab/superagent/contract"
	"github.com/shubolab/superagent/internal/durable"
)

type Scope struct {
	Subject contract.SubjectID
	Session string
	Runner  contract.RunnerRef
}

type Limits struct {
	MaxTurns        uint64
	MaxHistoryBytes uint64
}

type Reservation struct {
	Version       uint64 // Committed version from which the request begins.
	FirstVersion  uint64 // First included history turn, or Version+1 for empty history.
	Binding       string
	RequestDigest string
	Request       contract.StartRequest // History is reconstructed from committed turns.
	Attempted     bool
}

type ReservationKey struct {
	RunID         contract.RunID
	Version       uint64
	RequestDigest string
	Attempted     bool
}

func (r Reservation) Key() ReservationKey {
	return ReservationKey{r.Request.ID, r.Version, r.RequestDigest, r.Attempted}
}

type State struct {
	Version uint64
	Pending *Reservation
}

type Receipt struct {
	RunID   contract.RunID
	Version uint64
	Digest  string
}

type Turn struct {
	Receipt
	RequestDigest string
	Outcome       contract.RunOutcome
	Messages      []contract.Message
}

// CommitRecord contains at most one of Receipt (published) and Sealed (private).
// Both are nil when the authoritative store has neither record for the Run.
type CommitRecord struct {
	Receipt *Receipt
	Sealed  *Turn
}

var (
	ErrBusy              = contract.ErrSessionBusy
	ErrCapacity          = errors.New("session capacity reached")
	ErrHistoryLimit      = errors.New("latest turn exceeds the session history window")
	ErrOwnership         = errors.New("run or session ownership mismatch")
	ErrVersion           = errors.New("session version conflict")
	ErrContent           = errors.New("session run content conflict")
	ErrStorage           = errors.New("session storage unavailable")
	ErrResultUnavailable = errors.New("session run result unavailable")
)

// Store opens authenticated session storage. Pin the runner for each (Subject,
// Session) and reuse the same durable namespace when reopening it.
type Store interface {
	Open(context.Context, Scope, Limits) (Storage, error)
}

// Storage serves one session. Use authoritative reads, return independent
// snapshots, and serialize mutations across processes sharing this session.
type Storage interface {
	LoadState(context.Context) (State, error)
	ReadTurn(context.Context, uint64) (Turn, error) // Committed turns only, by immutable version.
	LookupCommit(context.Context, contract.RunID) (CommitRecord, error)
	// WithAdmission serializes short admission, resume, and discard operations.
	// Release the gate on callback exit or process failure. The SDK calls the
	// runner here, so keep this gate separate from storage write transactions.
	WithAdmission(context.Context, contract.RunID, func(context.Context) error) error
	// Reserve atomically checks capacity, the starting version, the pending slot,
	// and prior commits for this RunID. MarkAttempted persists before dispatch.
	Reserve(context.Context, Reservation) error
	MarkAttempted(context.Context, ReservationKey) error
	ClearReservation(context.Context, ReservationKey) error
	// Seal verifies the original reservation and persists an immutable delta.
	// Seal must accept the same delta after a concurrent Publish and reject a conflict.
	Seal(context.Context, Turn) error
	// Publish checks for a prior matching receipt first. Otherwise it atomically
	// makes the sealed delta visible, advances the version, and clears the pending slot.
	Publish(context.Context, contract.RunID, string) (Receipt, error)
}

// ResultDigest identifies the terminal delta stored by Seal and Publish.
func ResultDigest(outcome contract.RunOutcome, messages []contract.Message) (string, error) {
	return durable.Digest(struct {
		Outcome  contract.RunOutcome
		Messages []contract.Message
	}{outcome, messages})
}
