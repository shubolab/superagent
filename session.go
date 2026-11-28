package superagent

import (
	"context"

	"github.com/shubolab/superagent/internal/mesh"
	"github.com/shubolab/superagent/internal/session"
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
type Session struct {
	client *Client
	scope  session.Scope
	store  *session.Store
}

// OpenSession opens or creates an authenticated session in a private local directory.
func (c *Client) OpenSession(ctx context.Context, id SessionID, cfg SessionConfig) (*Session, error) {
	_, subject, err := c.sessionRunner(ctx)
	if err != nil {
		return nil, err
	}
	scope := session.Scope{Subject: subject, Session: string(id), Runner: c.runner}
	store, err := session.Open(ctx, cfg, scope)
	if err != nil {
		return nil, err
	}
	return &Session{client: c, scope: scope, store: store}, nil
}

// NewRunID allocates an opaque identifier for a new intent in this session.
func (s *Session) NewRunID() RunID { return s.store.NewRunID() }

// PendingRun returns the uncommitted RunID, or an empty ID when the session is idle.
func (s *Session) PendingRun(ctx context.Context) (RunID, error) {
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
	first, messages, err := s.store.Window(idx)
	return SessionSnapshot{Version: idx.Version, FirstVersion: first, Messages: messages}, err
}

// ReadTurn reads one complete committed delta by its session version.
func (s *Session) ReadTurn(ctx context.Context, version uint64) (SessionTurn, error) {
	idx, err := s.index(ctx)
	if err != nil {
		return SessionTurn{}, err
	}
	turn, err := s.store.ReadTurn(idx, version)
	return SessionTurn{Commit: turn.Receipt, Outcome: turn.Outcome, Messages: turn.Messages}, err
}

func (s *Session) index(ctx context.Context) (session.Index, error) {
	if _, err := s.runner(ctx); err != nil {
		return session.Index{}, err
	}
	return s.store.Load()
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

func (c *Client) sessionRunner(ctx context.Context) (mesh.SessionRunner, SubjectID, error) {
	local, ok := c.transport.(*mesh.Local)
	if !ok {
		return nil, "", ErrRecoveryUnavailable
	}
	return local.ResolveSessionRunner(ctx, c.runner)
}
