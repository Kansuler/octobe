// Package octobe provides a database abstraction layer focused on automatic transaction management
// and raw SQL execution without ORM complexity. It supports multiple database drivers while
// maintaining type safety through Go generics.
//
// The core philosophy is to eliminate boilerplate transaction management code while preserving
// the power and flexibility of raw SQL queries. Octobe uses the Handler pattern to encapsulate
// database operations in testable, composable functions.
//
// Basic usage:
//
//	db, err := octobe.New(postgres.OpenPGXPool(ctx, dsn))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	err = db.StartTransaction(ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
//	    user, err := session.Execute(ctx, CreateUser("Alice"))
//	    return err // Automatic rollback on error, commit on success
//	})
package octobe

import (
	"context"
	"errors"
	"fmt"
)

// ErrAlreadyUsed is returned when a single-use query segment is executed more than once.
var ErrAlreadyUsed = errors.New("segment has already been executed - segments can only be used once, create a new segment for additional queries")

// Option applies configuration to a driver config. Use this to customize
// transaction options, connection settings, or other driver-specific behavior.
//
// Example:
//
//	db.BeginTx(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{
//	    IsoLevel: pgx.ReadCommitted,
//	}))
type Option[CONFIG any] func(cfg *CONFIG)

// Driver owns database resources and creates sessions with type-safe configuration.
//
// Generic type parameters:
//   - DRIVER: The underlying database driver type (e.g., *sql.DB, *pgxpool.Pool)
//   - CONFIG: Configuration struct for driver options (e.g., transaction settings)
//   - BUILDER: Query builder type used by handlers
//
// Implementations create driver-specific Backend values and wrap them with NewSession.
// StartTransaction implementations can delegate managed transaction lifecycle to the
// package-level StartTransaction function.
type Driver[DRIVER any, CONFIG any, BUILDER any] interface {
	// Begin starts a new non-transactional database session.
	Begin(ctx context.Context) (*Session[BUILDER], error)

	// BeginTx starts a transactional database session.
	BeginTx(ctx context.Context, opts ...Option[CONFIG]) (*Session[BUILDER], error)

	// Close releases all database connections and resources.
	Close(ctx context.Context) error

	// Ping verifies database connectivity.
	Ping(ctx context.Context) error

	// StartTransaction executes fn within a transaction and owns commit and rollback.
	// The callback receives a ManagedSession so it cannot finalize the transaction directly.
	StartTransaction(ctx context.Context, fn func(session *ManagedSession[BUILDER]) error, opts ...Option[CONFIG]) (err error)
}

// Open is a deferred driver constructor. Driver packages use it to capture connection
// details or existing resources and create the configured Driver when called.
//
// Example:
//
//	opener := postgres.OpenPGXPool(ctx, "postgresql://user:pass@localhost/db")
//	db, err := octobe.New(opener)
type Open[DRIVER any, CONFIG any, BUILDER any] func() (Driver[DRIVER, CONFIG, BUILDER], error)

// New calls the provided Open function and returns its configured Driver.
//
// This is typically the first function called when setting up database access:
//
//	db, err := octobe.New(postgres.OpenPGXPool(ctx, dsn))
//	if err != nil {
//	    return fmt.Errorf("failed to initialize database: %w", err)
//	}
//	defer db.Close(ctx)
func New[DRIVER any, CONFIG any, BUILDER any](init Open[DRIVER, CONFIG, BUILDER]) (Driver[DRIVER, CONFIG, BUILDER], error) {
	driver, err := init()
	if err != nil {
		return nil, err
	}

	return driver, nil
}

// Backend is the driver-facing contract for an active session.
//
// Driver implementations pass a Backend to NewSession instead of implementing Session.
// Transactional backends must finalize and release their resources from Commit, Rollback,
// or Close. Non-transactional backends must reject Commit and Rollback and release their
// resources from Close.
type Backend[BUILDER any] interface {
	// Commit persists all changes made within the transaction.
	// Only valid for transactional sessions.
	Commit(ctx context.Context) error

	// Rollback discards all changes made within the transaction.
	// Only valid for transactional sessions.
	Rollback(ctx context.Context) error

	// Close releases session resources. For uncommitted transactional sessions,
	// Close rolls back the transaction. Close is idempotent.
	Close(ctx context.Context) error

	// Builder returns the query builder bound to this session.
	Builder() BUILDER
}

// NewSession wraps a driver Backend in the public Session API.
// It returns an error when backend is nil.
func NewSession[BUILDER any](backend Backend[BUILDER]) (*Session[BUILDER], error) {
	if backend == nil {
		return nil, errors.New("backend is nil")
	}

	return &Session[BUILDER]{
		backend: backend,
	}, nil
}

// Session is the public wrapper for a driver Backend. It provides handler execution,
// direct builder access, and manual lifecycle control.
type Session[BUILDER any] struct {
	backend Backend[BUILDER]
}

// Commit persists all changes made within the transaction.
// Only valid for transactional sessions.
func (s Session[BUILDER]) Commit(ctx context.Context) error {
	return s.backend.Commit(ctx)
}

// Rollback discards all changes made within the transaction.
// Only valid for transactional sessions.
func (s Session[BUILDER]) Rollback(ctx context.Context) error {
	return s.backend.Rollback(ctx)
}

// Close releases session resources. For uncommitted transactional sessions,
// Close rolls back the transaction. Close is idempotent.
func (s Session[BUILDER]) Close(ctx context.Context) error {
	return s.backend.Close(ctx)
}

// Builder returns the underlying builder for this session.
func (s Session[BUILDER]) Builder() BUILDER {
	return s.backend.Builder()
}

// StartTransaction executes fn within a database transaction, automatically handling commit/rollback.
//
// This is the recommended way to perform database operations as it:
// - Automatically begins a transaction
// - Calls fn with a transactional session
// - Commits on successful completion
// - Rolls back on any error or panic
// - Ensures proper cleanup in all cases
//
// The callback receives a ManagedSession that exposes handler execution methods but not
// Commit, Rollback, Close, or Builder. StartTransaction retains transaction ownership.
//
// Example:
//
//	err := db.StartTransaction(ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
//	    user, err := session.Execute(ctx, CreateUser("Alice"))
//	    if err != nil {
//	        return err // Automatic rollback
//	    }
//
//	    _, err = session.Execute(ctx, CreateProfile(user.ID))
//	    return err // Automatic commit if nil, rollback if error
//	})
func StartTransaction[DRIVER, CONFIG, BUILDER any](ctx context.Context, driver Driver[DRIVER, CONFIG, BUILDER], fn func(session *ManagedSession[BUILDER]) error, opts ...Option[CONFIG]) (err error) {
	session, err := driver.BeginTx(ctx, opts...)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = session.Rollback(ctx)
			panic(p)
		} else if err != nil {
			_ = session.Rollback(ctx)
		}
	}()

	err = fn(&ManagedSession[BUILDER]{session: session})
	if err != nil {
		return err
	}

	return session.backend.Commit(ctx)
}

// Handler processes database operations and returns typed results.
// Handlers receive the operation context explicitly and encapsulate SQL logic that can be
// easily tested by mocking the Builder.
//
// The Handler pattern provides several benefits:
// - Composable: handlers can be combined and reused
// - Testable: mock the builder to test SQL logic without a database
// - Type-safe: compile-time verification of return types
// - Transactional: automatic transaction management when used with StartTransaction
//
// Example:
//
//	func GetUser(id int) octobe.Handler[User, postgres.Builder] {
//	    return func(ctx context.Context, builder postgres.Builder) (User, error) {
//	        var user User
//	        query := builder(`SELECT id, name, email FROM users WHERE id = $1`)
//	        err := query.Arguments(id).QueryRow(ctx, &user.ID, &user.Name, &user.Email)
//	        return user, err
//	    }
//	}
type Handler[RESULT, BUILDER any] func(context.Context, BUILDER) (RESULT, error)

// VoidHandler processes a database operation that returns only an error.
//
// Example:
//
//	func UpdateUser(id int, name string) octobe.VoidHandler[postgres.Builder] {
//	    return func(ctx context.Context, builder postgres.Builder) error {
//	        query := builder(`UPDATE users SET name = $2 WHERE id = $1`)
//	        _, err := query.Arguments(id, name).Exec(ctx)
//	        return err
//	    }
//	}
type VoidHandler[BUILDER any] func(context.Context, BUILDER) error

// Execute runs an handler with the session's query builder.
//
// Example:
//
//	res, err := session.Execute(ctx, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func (s Session[BUILDER]) Execute[RESULT any](ctx context.Context, f Handler[RESULT, BUILDER]) (RESULT, error) {
	return f(ctx, s.backend.Builder())
}

// ExecuteVoid runs an error-only handler with the session's query builder.
//
// Example:
//
//	err := session.ExecuteVoid(ctx, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func (s Session[BUILDER]) ExecuteVoid(ctx context.Context, f VoidHandler[BUILDER]) error {
	return f(ctx, s.backend.Builder())
}

// ExecuteMany runs typed handlers in sequence within the same session.
// It stops at the first error and annotates it with the handler index. ExecuteMany does
// not start a transaction; atomicity depends on how the session was created.
//
// Example:
//
//	results, err := session.ExecuteMany(ctx,
//	    CreateUser("Alice"),
//	    CreateUser("Bob"),
//	    CreateUser("Charlie"),
//	)
func (s Session[BUILDER]) ExecuteMany[RESULT any](ctx context.Context, handlers ...Handler[RESULT, BUILDER]) ([]RESULT, error) {
	results := make([]RESULT, 0, len(handlers))
	for i, handler := range handlers {
		result, err := handler(ctx, s.backend.Builder())
		if err != nil {
			return nil, fmt.Errorf("handler %d failed: %w", i, err)
		}
		results = append(results, result)
	}
	return results, nil
}

// ManagedSession exposes handler execution inside a transaction managed by StartTransaction.
// It intentionally omits Commit, Rollback, Close, and Builder so the callback cannot interfere
// with transaction lifecycle management.
type ManagedSession[BUILDER any] struct {
	session *Session[BUILDER]
}

func (s ManagedSession[BUILDER]) Builder() BUILDER {
	return s.session.Builder()
}

// Execute runs an handler with the session's query builder.
//
// Example:
//
//	res, err := session.Execute(ctx, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func (s ManagedSession[BUILDER]) Execute[RESULT any](ctx context.Context, f Handler[RESULT, BUILDER]) (RESULT, error) {
	return f(ctx, s.session.Builder())
}

// ExecuteVoid runs an error-only handler with the session's query builder.
//
// Example:
//
//	err := session.ExecuteVoid(ctx, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func (s ManagedSession[BUILDER]) ExecuteVoid(ctx context.Context, f VoidHandler[BUILDER]) error {
	return f(ctx, s.session.Builder())
}

// ExecuteMany runs typed handlers in sequence within the same session.
// It stops at the first error and annotates it with the handler index. ExecuteMany does
// not start a transaction; atomicity depends on how the session was created.
//
// Example:
//
//	results, err := session.ExecuteMany(ctx,
//	    CreateUser("Alice"),
//	    CreateUser("Bob"),
//	    CreateUser("Charlie"),
//	)
func (s ManagedSession[BUILDER]) ExecuteMany[RESULT any](ctx context.Context, handlers ...Handler[RESULT, BUILDER]) ([]RESULT, error) {
	return s.session.ExecuteMany(ctx, handlers...)
}
