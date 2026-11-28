// Package session owns local session indexes and immutable Run message deltas.
package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/shubolab/superagent/internal/contract"
	"github.com/shubolab/superagent/internal/durable"
)

var (
	ErrBusy              = errors.New("session has an uncommitted run")
	ErrCapacity          = errors.New("session capacity reached")
	ErrHistoryLimit      = errors.New("latest turn exceeds the session history window")
	ErrOwnership         = errors.New("run or session ownership mismatch")
	ErrVersion           = errors.New("session version conflict")
	ErrContent           = errors.New("session run content conflict")
	ErrStorage           = errors.New("session storage unavailable")
	ErrResultUnavailable = errors.New("session run result unavailable")
)

type Config struct {
	Directory       string
	MaxTurns        uint64
	MaxHistoryBytes uint64
}

type Scope struct {
	Subject contract.SubjectID
	Session string
	Runner  contract.RunnerRef
}

type Receipt struct {
	RunID   contract.RunID
	Version uint64
	Digest  string
}

type Pending struct {
	Version       uint64
	FirstVersion  uint64
	Binding       string
	RequestDigest string
	Request       contract.StartRequest
	Attempted     bool
}

type Index struct {
	Format   int
	Scope    Scope
	Version  uint64
	Pending  *Pending
	Receipts []Receipt
}

type Turn struct {
	Receipt
	Scope         Scope
	RequestDigest string
	Outcome       contract.RunOutcome
	Messages      []contract.Message
}

type Store struct {
	cfg       Config
	scope     Scope
	indexPath string
	namespace string
}

func Open(ctx context.Context, cfg Config, scope Scope) (*Store, error) {
	// Reserve ample room per receipt in the fixed-size index.
	if !durable.Supported || cfg.MaxTurns == 0 || cfg.MaxTurns > durable.MaxRecordBytes/256 ||
		cfg.MaxHistoryBytes < 2 || cfg.MaxHistoryBytes > durable.MaxRecordBytes ||
		scope.Subject == "" || scope.Session == "" || scope.Runner == "" ||
		!utf8.ValidString(string(scope.Subject)) || !utf8.ValidString(scope.Session) || !utf8.ValidString(string(scope.Runner)) ||
		!filepath.IsAbs(cfg.Directory) {
		return nil, contract.ErrInvalidConfiguration
	}
	if err := durable.Directory(cfg.Directory); err != nil {
		return nil, storageError(err)
	}
	key, err := durable.Digest([2]string{string(scope.Subject), scope.Session})
	if err != nil {
		return nil, storageError(err)
	}
	namespace, err := durable.Digest(scope)
	if err != nil {
		return nil, storageError(err)
	}
	s := &Store{cfg: cfg, scope: scope, indexPath: filepath.Join(cfg.Directory, key+".session"), namespace: namespace}
	lock, err := s.Lock(ctx)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	if _, err := s.Load(); errors.Is(err, os.ErrNotExist) {
		if err := s.Save(Index{Format: 1, Scope: scope}); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) NewRunID() contract.RunID {
	var nonce [16]byte
	rand.Read(nonce[:])
	return contract.RunID(s.namespace + "." + hex.EncodeToString(nonce[:]))
}

func (s *Store) Owns(id contract.RunID) bool {
	suffix, ok := strings.CutPrefix(string(id), s.namespace+".")
	return ok && len(suffix) == 32 && strings.IndexFunc(suffix, func(r rune) bool {
		return (r < 'a' || r > 'f') && (r < '0' || r > '9')
	}) < 0
}

func (s *Store) Lock(ctx context.Context) (*os.File, error) {
	lock, err := durable.LockContext(ctx, s.indexPath)
	return lock, storageError(err)
}

func (s *Store) AdmissionLock(ctx context.Context, id contract.RunID) (*os.File, error) {
	if !s.Owns(id) {
		return nil, ErrOwnership
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := s.indexPath + ".admission"
	lock, err := durable.Lock(path)
	if !errors.Is(err, durable.ErrLocked) {
		return lock, storageError(err)
	}
	idx, err := s.Load()
	if err != nil {
		return nil, err
	}
	if idx.Pending != nil && idx.Pending.Request.ID != id {
		return nil, ErrBusy
	}
	lock, err = durable.LockContext(ctx, path)
	return lock, storageError(err)
}

func (s *Store) Load() (Index, error) {
	var idx Index
	if err := durable.Read(s.indexPath, &idx); err != nil {
		return idx, storageError(err)
	}
	if idx.Format != 1 || idx.Version != uint64(len(idx.Receipts)) {
		return idx, ErrStorage
	}
	if idx.Scope != s.scope {
		return idx, ErrOwnership
	}
	seen := make(map[contract.RunID]struct{}, len(idx.Receipts))
	for i, receipt := range idx.Receipts {
		_, duplicate := seen[receipt.RunID]
		if duplicate || !s.Owns(receipt.RunID) || receipt.Version != uint64(i)+1 || !validDigest(receipt.Digest) {
			return idx, ErrStorage
		}
		seen[receipt.RunID] = struct{}{}
	}
	if p := idx.Pending; p != nil {
		_, committed := seen[p.Request.ID]
		if committed || !s.Owns(p.Request.ID) || !p.Request.Recoverable || len(p.Request.History) != 0 ||
			p.FirstVersion == 0 || p.FirstVersion > idx.Version+1 || !validDigest(p.RequestDigest) || !validDigest(p.Binding) {
			return idx, ErrStorage
		}
	}
	return idx, nil
}

func (s *Store) Save(idx Index) error { return s.write(s.indexPath, idx) }

func (s *Store) CheckCapacity(idx Index, p Pending) error {
	if idx.Version >= s.cfg.MaxTurns {
		return ErrCapacity
	}
	// Check both the current reservation and the next published index before dispatch.
	idx.Pending = &p
	for _, next := range []Index{idx, {
		Format: idx.Format, Scope: idx.Scope, Version: idx.Version + 1,
		Receipts: append(idx.Receipts, Receipt{RunID: p.Request.ID, Version: idx.Version + 1, Digest: p.RequestDigest}),
	}} {
		data, err := json.Marshal(next)
		if err != nil {
			return fmt.Errorf("%w: %w", contract.ErrInvalidRequest, err)
		}
		if len(data) > durable.MaxRecordBytes {
			return ErrCapacity
		}
	}
	return nil
}

func (s *Store) Sync() error {
	return storageError(durable.SyncDirectory(s.cfg.Directory))
}

func (s *Store) write(path string, value any) error {
	renamed, err := durable.Write(path, value)
	if err != nil && renamed {
		// The caller holds the index lock, so a successful directory sync confirms this write.
		err = s.Sync()
	}
	return storageError(err)
}

func (idx Index) Receipt(id contract.RunID) (Receipt, bool) {
	for _, receipt := range idx.Receipts {
		if receipt.RunID == id {
			return receipt, true
		}
	}
	return Receipt{}, false
}

func (idx Index) Awaiting(id contract.RunID) (Pending, error) {
	if idx.Pending == nil || idx.Pending.Request.ID != id {
		return Pending{}, ErrOwnership
	}
	if idx.Pending.Version != idx.Version {
		return Pending{}, ErrVersion
	}
	return *idx.Pending, nil
}

func (s *Store) turnPath(id contract.RunID) string { return s.indexPath + "." + string(id) + ".turn" }

func (s *Store) ReadTurn(idx Index, version uint64) (Turn, error) {
	if version == 0 || version > idx.Version {
		return Turn{}, ErrVersion
	}
	receipt := idx.Receipts[version-1]
	turn, err := s.readTurn(receipt.RunID)
	if err != nil {
		return Turn{}, err
	}
	if turn.Receipt != receipt {
		return Turn{}, ErrStorage
	}
	return turn, nil
}

func (s *Store) readTurn(id contract.RunID) (Turn, error) {
	var turn Turn
	if err := durable.Read(s.turnPath(id), &turn); err != nil {
		return Turn{}, storageError(err)
	}
	digest, err := resultDigest(turn.Outcome, turn.Messages)
	if err != nil || turn.Scope != s.scope || turn.RunID != id || turn.Digest != digest || !validDigest(turn.RequestDigest) {
		return Turn{}, ErrStorage
	}
	return turn, nil
}

func resultDigest(outcome contract.RunOutcome, messages []contract.Message) (string, error) {
	return durable.Digest(struct {
		Outcome  contract.RunOutcome
		Messages []contract.Message
	}{outcome, messages})
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func storageError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrStorage) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrStorage, err)
}
