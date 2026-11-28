package sdk

import (
	"context"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/session"
	"github.com/shubolab/superagent/mesh"
	api "github.com/shubolab/superagent/session"
)

type SessionID string

type SessionConfig = session.Config
type SessionCommit = session.Receipt

type SessionSnapshot struct {
	Version      uint64
	FirstVersion uint64
	Messages     []contract.Message
}

type SessionTurn struct {
	Commit   SessionCommit
	Outcome  contract.RunOutcome
	Messages []contract.Message
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
type Session struct {
	client    *Client
	scope     session.Scope
	store     api.Storage
	namespace string
	limits    api.Limits
}

// OpenSession opens or creates an authenticated session in a private local directory.
func (c *Client) OpenSession(ctx context.Context, id SessionID, cfg SessionConfig) (*Session, error) {
	return c.OpenSessionWithStore(ctx, id, session.Files{Directory: cfg.Directory}, api.Limits{MaxTurns: cfg.MaxTurns, MaxHistoryBytes: cfg.MaxHistoryBytes})
}

// OpenSessionWithStore opens an authenticated session using the host's backend.
func (c *Client) OpenSessionWithStore(ctx context.Context, id SessionID, backend api.Store, limits api.Limits) (*Session, error) {
	_, subject, err := c.sessionRunner(ctx)
	if err != nil {
		return nil, err
	}
	scope := session.Scope{Subject: subject, Session: string(id), Runner: c.runner}
	if !session.ValidScope(scope, limits) || nilBinding(backend) {
		return nil, contract.ErrInvalidConfiguration
	}
	store, err := backend.Open(ctx, scope, limits)
	if err != nil {
		return nil, err
	}
	if nilBinding(store) {
		return nil, contract.ErrInvalidConfiguration
	}
	return &Session{client: c, scope: scope, store: store, namespace: session.Namespace(scope), limits: limits}, nil
}

// NewRunID allocates an opaque identifier for a new intent in this session.
func (s *Session) NewRunID() contract.RunID { return session.NewRunID(s.namespace) }

func (s *Session) owns(id contract.RunID) bool { return session.Owns(s.namespace, id) }

// PendingRun returns the uncommitted RunID, or an empty ID when the session is idle.
func (s *Session) PendingRun(ctx context.Context) (contract.RunID, error) {
	idx, err := s.index(ctx)
	if err != nil || idx.Pending == nil {
		return "", err
	}
	return idx.Pending.Request.ID, nil
}

// Snapshot reads the bounded history window used for the next Run.
func (s *Session) Snapshot(ctx context.Context) (SessionSnapshot, error) {
	idx, err := s.index(ctx)
	if err != nil {
		return SessionSnapshot{}, err
	}
	first, messages, err := s.window(ctx, idx.Version, 1, s.limits.MaxHistoryBytes, false)
	return SessionSnapshot{Version: idx.Version, FirstVersion: first, Messages: messages}, err
}

// ReadTurn reads one complete committed delta by its session version.
func (s *Session) ReadTurn(ctx context.Context, version uint64) (SessionTurn, error) {
	idx, err := s.index(ctx)
	if err != nil {
		return SessionTurn{}, err
	}
	if version == 0 || version > idx.Version {
		return SessionTurn{}, ErrSessionVersion
	}
	turn, err := s.readTurn(ctx, version)
	return SessionTurn{Commit: SessionCommit(turn.Receipt), Outcome: turn.Outcome, Messages: turn.Messages}, err
}

func (s *Session) index(ctx context.Context) (api.State, error) {
	if _, err := s.runner(ctx); err != nil {
		return api.State{}, err
	}
	return s.store.LoadState(ctx)
}

func (s *Session) readTurn(ctx context.Context, version uint64) (api.Turn, error) {
	turn, err := s.store.ReadTurn(ctx, version)
	if err != nil {
		return api.Turn{}, err
	}
	digest, err := api.ResultDigest(turn.Outcome, turn.Messages)
	if err != nil || turn.Version != version || !s.owns(turn.RunID) || digest != turn.Digest {
		return api.Turn{}, ErrSessionStorage
	}
	return turn, nil
}

func (s *Session) window(ctx context.Context, version, first, limit uint64, exact bool) (uint64, []contract.Message, error) {
	if files, ok := s.store.(session.Backend); ok {
		return files.ReadWindow(ctx, version, first, limit, exact)
	}
	return session.ReadWindow(version, first, limit, exact, func(at uint64) ([]contract.Message, error) {
		turn, err := s.readTurn(ctx, at)
		return turn.Messages, err
	})
}

func awaiting(state api.State, id contract.RunID) (api.Reservation, error) {
	if state.Pending == nil || state.Pending.Request.ID != id {
		return api.Reservation{}, ErrSessionOwnership
	}
	if state.Pending.Version != state.Version {
		return api.Reservation{}, ErrSessionVersion
	}
	return *state.Pending, nil
}

func (s *Session) runner(ctx context.Context) (mesh.SessionRunner, error) {
	runner, subject, err := s.client.sessionRunner(ctx)
	if err != nil {
		return nil, err
	}
	if subject != s.scope.Subject {
		return nil, ErrSessionOwnership
	}
	return runner, nil
}

func (c *Client) sessionRunner(ctx context.Context) (mesh.SessionRunner, contract.SubjectID, error) {
	resolver, ok := c.transport.(mesh.SessionResolver)
	if !ok {
		return nil, "", contract.ErrRecoveryUnavailable
	}
	return resolver.ResolveSessionRunner(ctx, c.runner)
}
