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
//	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
//	    user, err := octobe.Execute(ctx, session, CreateUser("Alice"))
//	    return err // Automatic rollback on error, commit on success
//	})
package octobe

import (
	"context"
	"errors"
	"fmt"
)

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

// Driver manages database connections and sessions with type-safe configuration.
//
// Generic type parameters:
//   - DRIVER: The underlying database driver type (e.g., *sql.DB, *pgxpool.Pool)
//   - CONFIG: Configuration struct for driver options (e.g., transaction settings)
//   - BUILDER: Query builder type that constructs executable queries
//
// Implementations handle connection pooling, transaction lifecycle, and driver-specific
// optimizations while providing a consistent interface across database types.
type Driver[DRIVER any, CONFIG any, BUILDER any] interface {
	// Begin starts a new non-transactional database session.
	Begin(ctx context.Context) (Session[BUILDER], error)

	// BeginTx starts a transactional database session.
	BeginTx(ctx context.Context, opts ...Option[CONFIG]) (Session[BUILDER], error)

	// Close releases all database connections and resources.
	Close(ctx context.Context) error

	// Ping verifies database connectivity.
	Ping(ctx context.Context) error

	// StartTransaction executes fn within a transaction, automatically handling commit/rollback.
	StartTransaction(ctx context.Context, fn func(session BuilderSession[BUILDER]) error, opts ...Option[CONFIG]) (err error)
}

// Open initializes and returns a configured driver instance. This function type
// encapsulates driver creation logic including connection string parsing,
// pool configuration, and initial connectivity validation.
//
// Example:
//
//	opener := postgres.OpenPGXPool(ctx, "postgresql://user:pass@localhost/db")
//	db, err := octobe.New(opener)
type Open[DRIVER any, CONFIG any, BUILDER any] func() (Driver[DRIVER, CONFIG, BUILDER], error)

// New creates a new Octobe instance using the provided driver opener function.
// The opener is called immediately to initialize the underlying driver and
// establish database connectivity.
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

// Session represents an active database session that may or may not be transactional.
//
// Transactional sessions (created with transaction options) maintain ACID properties
// and must call Commit(ctx) to persist changes or Rollback(ctx) or Close(ctx) to discard them.
// Non-transactional sessions execute queries immediately without transaction boundaries
// and must call Close(ctx) to release session resources.
//
// Sessions embed BuilderSession to provide direct access to query construction methods.
type Session[BUILDER any] interface {
	// Commit persists all changes made within the transaction.
	// Only valid for transactional sessions.
	Commit(ctx context.Context) error

	// Rollback discards all changes made within the transaction.
	// Only valid for transactional sessions.
	Rollback(ctx context.Context) error

	// Close releases session resources. For uncommitted transactional sessions,
	// Close rolls back the transaction. Close is idempotent.
	Close(ctx context.Context) error

	BuilderSession[BUILDER]
}

// BuilderSession provides access to the query builder for constructing database operations.
// This interface is embedded in Session and used directly by StartTransaction for
// automatic transaction management.
//
// The Builder creates Segment instances that represent prepared queries with arguments.
type BuilderSession[BUILDER any] interface {
	// Builder returns a query builder function for this session.
	// Each call to Builder() creates segments scoped to this session.
	Builder() BUILDER
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
// The function parameter receives a BuilderSession that can be used to execute
// multiple related database operations within the same transaction.
//
// Example:
//
//	err := db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
//	    user, err := octobe.Execute(ctx, session, CreateUser("Alice"))
//	    if err != nil {
//	        return err // Automatic rollback
//	    }
//
//	    _, err = octobe.Execute(ctx, session, CreateProfile(user.ID))
//	    return err // Automatic commit if nil, rollback if error
//	})
func StartTransaction[DRIVER, CONFIG, BUILDER any](ctx context.Context, driver Driver[DRIVER, CONFIG, BUILDER], fn func(session BuilderSession[BUILDER]) error, opts ...Option[CONFIG]) (err error) {
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

	err = fn(session)
	if err != nil {
		return err
	}

	return session.Commit(ctx)
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

// VoidHandler is like Handler but returns an error only. Ignores any result value.
//
// Example:
//
//	func GetUser(id int, name string) octobe.VoidHandler[postgres.Builder] {
//	    return func(ctx context.Context, builder postgres.Builder) error {
//	        query := builder(`UPDATE SET name = $2 FROM users WHERE id = $1`)
//	        _, err := query.Arguments(id, name).Exec(ctx)
//	        return err
//	    }
//	}
type VoidHandler[BUILDER any] func(context.Context, BUILDER) error

// Execute runs a handler function with the session's query builder.
func Execute[RESULT, BUILDER any](ctx context.Context, session BuilderSession[BUILDER], f Handler[RESULT, BUILDER]) (RESULT, error) {
	return f(ctx, session.Builder())
}

// ExecuteVoid runs a void handler (one that returns octobe.Void) and returns only the error.
// This provides cleaner syntax for operations that don't return data.
//
// Example:
//
//	err := octobe.ExecuteVoid(ctx, session, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func ExecuteVoid[BUILDER any](ctx context.Context, session BuilderSession[BUILDER], f VoidHandler[BUILDER]) error {
	return f(ctx, session.Builder())
}

// ExecuteMany runs multiple handlers in sequence within the same session.
// If any handler fails, execution stops and the error is returned.
// This is useful for running related operations that should succeed or fail together.
//
// Example:
//
//	results, err := octobe.ExecuteMany(ctx, session,
//	    CreateUser("Alice"),
//	    CreateUser("Bob"),
//	    CreateUser("Charlie"),
//	)
func ExecuteMany[RESULT, BUILDER any](ctx context.Context, session BuilderSession[BUILDER], handlers ...Handler[RESULT, BUILDER]) ([]RESULT, error) {
	results := make([]RESULT, 0, len(handlers))
	for i, handler := range handlers {
		result, err := handler(ctx, session.Builder())
		if err != nil {
			return nil, fmt.Errorf("handler %d failed: %w", i, err)
		}
		results = append(results, result)
	}
	return results, nil
}
