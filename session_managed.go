package octobe

import (
	"errors"
	"context"
)

// NewSession wraps a driver Backend in the public Session API.
// It returns an error when backend is nil.
func NewSessionManaged[QF any](backend Backend[QF]) (*SessionManaged[QF], error) {
	if backend == nil {
		return nil, errors.New("backend is nil")
	}

	return &SessionManaged[QF]{
		backend: backend,
	}, nil
}

// Session is the public wrapper for a driver Backend. It provides handler execution,
// direct query-factory access, and manual lifecycle control.
type SessionManaged[QF any] struct {
	backend Backend[QF]
}

// Execute runs a handler with the session's query factory.
//
// Example:
//
//	res, err := session.Execute(ctx, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func (s *SessionManaged[QF]) Execute[R any](ctx context.Context, handler Handler[R, QF]) (R, error) {
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
func (s *SessionManaged[QF]) ExecuteNoResult(ctx context.Context, handler NoResultHandler[QF]) error {
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
func (s *SessionManaged[QF]) ExecuteSequence[R any](ctx context.Context, handlers ...Handler[R, QF]) ([]R, error) {
	return executeSequence(ctx, s.backend.QueryFactory(), handlers...)
}
