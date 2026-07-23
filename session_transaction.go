package octobe

import (
	"errors"
	"context"
)

// NewTransaction wraps a driver Backend in the public Session API.
// It returns an error when backend is nil.
func NewTransaction[QF any](backend Backend[QF]) (*SessionTransaction[QF], error) {
	if backend == nil {
		return nil, errors.New("backend is nil")
	}

	return &SessionTransaction[QF]{
		backend: backend,
	}, nil
}

// Session is the public wrapper for a driver Backend. It provides handler execution,
// direct query-factory access, and manual lifecycle control.
type SessionTransaction[QF any] struct {
	backend Backend[QF]
}

// Commit persists all changes made within the transaction.
// Only valid for transactional sessions.
func (s SessionTransaction[QF]) Commit(ctx context.Context) error {
	return s.backend.Commit(ctx)
}

// Rollback discards all changes made within the transaction.
// Only valid for transactional sessions.
func (s SessionTransaction[QF]) Rollback(ctx context.Context) error {
	return s.backend.Rollback(ctx)
}

// Execute runs a handler with the session's query factory.
//
// Example:
//
//	res, err := session.Execute(ctx, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func (s SessionTransaction[QF]) Execute[R any](ctx context.Context, handler Handler[R, QF]) (R, error) {
	return handler(ctx, s.backend.QueryFactory())
}

// ExecuteNoResult runs an error-only handler with the session's query factory.
//
// Example:
//
//	err := session.ExecuteNoResult(ctx, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func (s SessionTransaction[QF]) ExecuteNoResult(ctx context.Context, handler NoResultHandler[QF]) error {
	return handler(ctx, s.backend.QueryFactory())
}

// ExecuteSequence runs typed handlers in sequence within the same session.
// It stops at the first error and annotates it with the handler index. ExecuteSequence does
// not start a transaction; atomicity depends on how the session was created.
//
// Example:
//
//	results, err := session.ExecuteSequence(ctx,
//	    CreateUser("Alice"),
//	    CreateUser("Bob"),
//	    CreateUser("Charlie"),
//	)
func (s SessionTransaction[QF]) ExecuteSequence[R any](ctx context.Context, handlers ...Handler[R, QF]) ([]R, error) {
	return executeSequence(ctx, s.backend.QueryFactory(), handlers...)
}
