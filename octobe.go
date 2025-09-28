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
//	    user, err := postgres.Execute(session, CreateUser("Alice"))
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
//	db.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{
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
	// Begin starts a new database session. If transaction options are provided,
	// the session will be transactional and require Commit/Rollback.
	Begin(ctx context.Context, opts ...Option[CONFIG]) (Session[BUILDER], error)

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
// and must call Commit() to persist changes or Rollback() to discard them.
// Non-transactional sessions execute queries immediately without transaction boundaries.
//
// Sessions embed BuilderSession to provide direct access to query construction methods.
type Session[BUILDER any] interface {
	// Commit persists all changes made within the transaction.
	// Only valid for transactional sessions.
	Commit() error

	// Rollback discards all changes made within the transaction.
	// Only valid for transactional sessions.
	Rollback() error

	BuilderSession[BUILDER]
}

// BuilderSession provides access to the query builder for constructing database operations.
// This interface is embedded in Session and used directly by StartTransaction for
// automatic transaction management.
//
// The Builder creates Segment instances that represent prepared queries with arguments.
type BuilderSession[BUILDER any] interface {
	// Builder returns a query builder function for this session.
	// Each call to Builder() creates segments that are scoped to this session's
	// transaction (if transactional) or connection (if non-transactional).
	Builder() BUILDER
}

// Void represents an empty return type for handlers that perform actions without returning data.
// Use this for operations like INSERT, UPDATE, DELETE that only need to report success/failure.
//
// Example:
//
//	func DeleteUser(id int) postgres.Handler[octobe.Void] {
//	    return func(builder postgres.Builder) (octobe.Void, error) {
//	        query := builder(`DELETE FROM users WHERE id = $1`)
//	        _, err := query.Arguments(id).Exec()
//	        return nil, err
//	    }
//	}
type Void *struct{}

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
//	    user, err := postgres.Execute(session, CreateUser("Alice"))
//	    if err != nil {
//	        return err // Automatic rollback
//	    }
//
//	    _, err = postgres.Execute(session, CreateProfile(user.ID))
//	    return err // Automatic commit if nil, rollback if error
//	})
func StartTransaction[DRIVER, CONFIG, BUILDER any](ctx context.Context, driver Driver[DRIVER, CONFIG, BUILDER], fn func(session BuilderSession[BUILDER]) error, opts ...Option[CONFIG]) (err error) {
	session, err := driver.Begin(ctx, opts...)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = session.Rollback()
			panic(p)
		} else if err != nil {
			_ = session.Rollback()
		}
	}()

	err = fn(session)
	if err != nil {
		return err
	}

	err = session.Commit()
	return err
}

// Handler processes database operations and returns typed results.
// Handlers encapsulate SQL logic and can be easily tested by mocking the Builder.
//
// The Handler pattern provides several benefits:
// - Composable: handlers can be combined and reused
// - Testable: mock the builder to test SQL logic without a database
// - Type-safe: compile-time verification of return types
// - Transactional: automatic transaction management when used with StartTransaction
//
// Example:
//
//	func GetUser(id int) Handler[User] {
//	    return func(builder Builder) (User, error) {
//	        var user User
//	        query := builder(`SELECT id, name, email FROM users WHERE id = $1`)
//	        err := query.Arguments(id).QueryRow(&user.ID, &user.Name, &user.Email)
//	        return user, err
//	    }
//	}
type Handler[RESULT, BUILDER any] func(BUILDER) (RESULT, error)

// Execute runs a handler function with the session's query builder.
func Execute[RESULT, BUILDER any](session BuilderSession[BUILDER], f Handler[RESULT, BUILDER]) (RESULT, error) {
	return f(session.Builder())
}

// ExecuteVoid runs a void handler (one that returns octobe.Void) and returns only the error.
// This provides cleaner syntax for operations that don't return data.
//
// Example:
//
//	err := postgres.ExecuteVoid(session, DeleteUser(123))
//	if err != nil {
//	    return fmt.Errorf("failed to delete user: %w", err)
//	}
func ExecuteVoid[BUILDER any](session BuilderSession[BUILDER], f Handler[Void, BUILDER]) error {
	_, err := f(session.Builder())
	return err
}

// ExecuteMany runs multiple handlers in sequence within the same session.
// If any handler fails, execution stops and the error is returned.
// This is useful for running related operations that should succeed or fail together.
//
// Example:
//
//	results, err := postgres.ExecuteMany(session,
//	    CreateUser("Alice"),
//	    CreateUser("Bob"),
//	    CreateUser("Charlie"),
//	)
func ExecuteMany[RESULT, BUILDER any](session BuilderSession[BUILDER], handlers ...Handler[RESULT, BUILDER]) ([]RESULT, error) {
	results := make([]RESULT, 0, len(handlers))
	for i, handler := range handlers {
		result, err := handler(session.Builder())
		if err != nil {
			return nil, fmt.Errorf("handler %d failed: %w", i, err)
		}
		results = append(results, result)
	}
	return results, nil
}
