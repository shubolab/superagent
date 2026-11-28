package superagent

import (
	"context"

	"github.com/shubolab/superagent/internal/session"
	"github.com/shubolab/superagent/sdk"
	api "github.com/shubolab/superagent/session"
)

type SessionID string

type SessionConfig = session.Config
type SessionCommit = session.Receipt

type SessionSnapshot struct {
	Version      uint64
	FirstVersion uint64
	Messages     []Message
}

type SessionTurn struct {
	Commit   SessionCommit
	Outcome  RunOutcome
	Messages []Message
}

var (
	ErrSessionBusy              = session.ErrBusy
	ErrSessionCapacity          = session.ErrCapacity
	ErrSessionHistoryLimit      = session.ErrHistoryLimit
	ErrSessionOwnership         = session.ErrOwnership
	ErrSessionVersion           = session.ErrVersion
	ErrSessionContent           = session.ErrContent
	ErrSessionStorage           = session.ErrStorage
	ErrSessionResultUnavailable = session.ErrResultUnavailable
)

// Session manages durable history for one subject and logical runner.
type Session sdk.Session

// OpenSession opens or creates an authenticated session in a private local directory.
func (c *Client) OpenSession(ctx context.Context, id SessionID, cfg SessionConfig) (*Session, error) {
	s, err := (*sdk.Client)(c).OpenSession(ctx, sdk.SessionID(id), cfg)
	return (*Session)(s), err
}

// OpenSessionWithStore opens an authenticated session using the host's backend.
func (c *Client) OpenSessionWithStore(ctx context.Context, id SessionID, store api.Store, limits api.Limits) (*Session, error) {
	s, err := (*sdk.Client)(c).OpenSessionWithStore(ctx, sdk.SessionID(id), store, limits)
	return (*Session)(s), err
}

// NewRunID allocates an opaque identifier for a new intent in this session.
func (s *Session) NewRunID() RunID { return (*sdk.Session)(s).NewRunID() }

// PendingRun returns the uncommitted RunID, or an empty ID when the session is idle.
func (s *Session) PendingRun(ctx context.Context) (RunID, error) {
	return (*sdk.Session)(s).PendingRun(ctx)
}

// Snapshot reads the bounded history window used for the next Run.
func (s *Session) Snapshot(ctx context.Context) (SessionSnapshot, error) {
	snapshot, err := (*sdk.Session)(s).Snapshot(ctx)
	return SessionSnapshot(snapshot), err
}

// ReadTurn reads one complete committed delta by its session version.
func (s *Session) ReadTurn(ctx context.Context, version uint64) (SessionTurn, error) {
	turn, err := (*sdk.Session)(s).ReadTurn(ctx, version)
	return SessionTurn(turn), err
}

// Start reserves this session's next Run and uses its committed history.
// The caller supplies a NewRunID and the current input. The session owns History and Recoverable.
func (s *Session) Start(ctx context.Context, req StartRequest) (*Run, error) {
	run, err := (*sdk.Session)(s).Start(ctx, req)
	return (*Run)(run), err
}

// Recover reconciles the original pending Run against its retained local authority.
func (s *Session) Recover(ctx context.Context, id RunID) (*Run, error) {
	run, err := (*sdk.Session)(s).Recover(ctx, id)
	return (*Run)(run), err
}

// CommitRun publishes the original terminal Wait delta once and returns its durable receipt.
// A completed receipt or sealed delta remains usable after the runner's result retention expires.
func (s *Session) CommitRun(ctx context.Context, id RunID) (SessionCommit, error) {
	return (*sdk.Session)(s).CommitRun(ctx, id)
}

type StartPhase string

const (
	StartQueued    StartPhase = StartPhase(sdk.StartQueued)
	StartAdmitting StartPhase = StartPhase(sdk.StartAdmitting)
	StartResolved  StartPhase = StartPhase(sdk.StartResolved)
)

// StartUpdate is one admission milestone or the final outcome of the same Start.
type StartUpdate struct {
	RunID RunID
	Phase StartPhase
	Run   *Run
	Err   error
}

// StartWithProgress freezes the input before returning and closes its bounded stream
// after the final reply. A slow reader does not delay admission.
func (s *Session) StartWithProgress(ctx context.Context, req StartRequest) <-chan StartUpdate {
	return sdk.StartWithProgress((*sdk.Session)(s), ctx, req, startUpdate)
}

func startUpdate(id RunID, phase sdk.StartPhase, run *sdk.Run, err error) StartUpdate {
	return StartUpdate{RunID: id, Phase: StartPhase(phase), Run: (*Run)(run), Err: err}
}

type PendingRecoveryState string

const (
	RecoveryIdle       PendingRecoveryState = PendingRecoveryState(sdk.RecoveryIdle)
	RecoverySave       PendingRecoveryState = PendingRecoveryState(sdk.RecoverySave)
	RecoveryContinue   PendingRecoveryState = PendingRecoveryState(sdk.RecoveryContinue)
	RecoveryUnadmitted PendingRecoveryState = PendingRecoveryState(sdk.RecoveryUnadmitted)
	RecoveryUnknown    PendingRecoveryState = PendingRecoveryState(sdk.RecoveryUnknown)
)

// ErrPendingResultReady means the original Run can be saved without resuming it.
var ErrPendingResultReady = sdk.ErrPendingResultReady

type PendingRecovery struct {
	RunID RunID
	State PendingRecoveryState
	Cause error
	Draft string // Populated only after successfully discarding an unadmitted reservation.
}

// InspectPending classifies retained evidence without writing records or dispatching work.
func (s *Session) InspectPending(ctx context.Context) (PendingRecovery, error) {
	pending, err := (*sdk.Session)(s).InspectPending(ctx)
	return pendingRecovery(pending), err
}

// CommitPendingResult only publishes a retained terminal result. It never resumes a Run.
func (s *Session) CommitPendingResult(ctx context.Context, id RunID) (SessionCommit, error) {
	return (*sdk.Session)(s).CommitPendingResult(ctx, id)
}

// DiscardUnadmitted clears only the exact reservation proven never to have attempted admission.
func (s *Session) DiscardUnadmitted(ctx context.Context, id RunID) (PendingRecovery, error) {
	pending, err := (*sdk.Session)(s).DiscardUnadmitted(ctx, id)
	return pendingRecovery(pending), err
}

func pendingRecovery(pending sdk.PendingRecovery) PendingRecovery {
	return PendingRecovery{RunID: pending.RunID, State: PendingRecoveryState(pending.State), Cause: pending.Cause, Draft: pending.Draft}
}

// ResumeAdmittedWithProgress continues an already admitted Run using its original checkpoint.
// An expired or missing checkpoint cannot turn this operation into a fresh Start.
func (s *Session) ResumeAdmittedWithProgress(ctx context.Context, id RunID) <-chan StartUpdate {
	return sdk.ResumeAdmittedWithProgress((*sdk.Session)(s), ctx, id, startUpdate)
}
