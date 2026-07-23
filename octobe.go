// Package octobe provides a database abstraction layer focused on automatic transaction management
// and raw SQL execution without ORM complexity. It supports multiple database drivers while
// maintaining type safety through Go generics.
//
// The core philosophy is to eliminate boilerplate transaction management code while preserving
// the power and flexibility of raw SQL queries. Octobe uses the Handlers similar to go's standard library's
// http handler pattern to encapsulate database operations in testable, composable functions.
//
// Basic usage:
//
//	db, err := octobe.New(postgres.OpenPGXPool(ctx, dsn))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Run a managed transaction
//	err = db.RunInTransaction(ctx, func(session *octobe.ManagedSession[postgres.QueryFactory]) error {
//		user, err := session.Execute(ctx, CreateUser("Alice"))
//		return err // Automatic rollback on error, commit on success
//	})
//
//	// Manage your own transaction
//	tx, err := db.Transaction(ctx, postgres.PGXTxOptions{})
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer tx.Rollback(ctx)
//
//	user, err := tx.Execute(ctx, CreateUser("Alice"))
//	if err != nil {
//		return err
//	}
//
//	 return tx.Commit(ctx)
package octobe

import (
	"context"
	"errors"
	"fmt"
)

// ErrStatementAlreadyExecuted is returned when a single-use statement is executed more than once.
var ErrStatementAlreadyExecuted = errors.New("statement has already been executed - statements can only be executed once, create a new statement for additional queries")

// Option applies configuration to a driver config. Use this to customize
// transaction options, connection settings, or other driver-specific behavior.
//
// Example:
//
//	db.BeginTx(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{
//	    IsoLevel: pgx.ReadCommitted,
//	}))
type Option[Config any] func(cfg *Config)

// Driver owns database resources and creates sessions with type-safe configuration.
//
// Generic type parameters:
//   - D: The underlying database driver type (e.g., *sql.DB, *pgxpool.Pool)
//   - C: Configuration struct for driver options (e.g., transaction settings)
//   - QF: Query factory type used by handlers
//
// Implementations create driver-specific Backend values and wrap them with NewSession.
// RunInTransaction implementations can delegate managed transaction lifecycle to the
// package-level RunInTransaction function.
type Driver[D any, C any, QF any] interface {
	// OpenSession opens a new non-transactional database session.
	Session(ctx context.Context) (*Session[QF], error)

	// Transaction starts a transactional database session.
	Transaction(ctx context.Context, opts ...Option[C]) (*SessionTransaction[QF], error)

	// Close releases all database connections and resources.
	Close(ctx context.Context) error

	// Ping verifies database connectivity.
	Ping(ctx context.Context) error

	// RunInTransaction executes fn within a transaction and owns commit and rollback.
	// The callback receives a ManagedSession so it cannot finalize the transaction directly.
	RunInTransaction(ctx context.Context, fn func(session *SessionManaged[QF]) error, opts ...Option[C]) (err error)
}

// OpenFunc is a deferred driver constructor. Driver packages use it to capture connection
// details or existing resources and create the configured Driver when called.
//
// Example:
//
//	opener := postgres.OpenPGXPool(ctx, "postgresql://user:pass@localhost/db")
//	db, err := octobe.New(opener)
type OpenFunc[D any, C any, QF any] func() (Driver[D, C, QF], error)

// New calls the provided OpenFunc and returns its configured Driver.
//
// This is typically the first function called when setting up database access:
//
//	db, err := octobe.New(postgres.OpenPGXPool(ctx, dsn))
//	if err != nil {
//	    return fmt.Errorf("failed to initialize database: %w", err)
//	}
//	defer db.Close(ctx)
func New[D any, C any, QF any](open OpenFunc[D, C, QF]) (Driver[D, C, QF], error) {
	driver, err := open()
	if err != nil {
		return nil, err
	}

	return driver, nil
}

// Backend is the driver-facing contract for an active session.
type Backend[QF any] interface {
	// Commit persists all changes made within the transaction.
	Commit(ctx context.Context) error

	// Rollback discards all changes made within the transaction.
	Rollback(ctx context.Context) error

	// Close releases session resources.
	Close(ctx context.Context) error

	// QueryFactory returns the query factory bound to this session.
	QueryFactory() QF
}

// RunInTransaction executes fn within a database transaction, automatically handling commit/rollback.
// If the callback returns an error, the transaction is rolled back. The callback receives a ManagedSession
// that exposes handler execution methods but not Commit, Rollback, or Close. RunInTransaction retains
// transaction ownership.
//
// Example:
//
//	err := db.RunInTransaction(ctx, func(session *octobe.ManagedSession[postgres.QueryFactory]) error {
//	    user, err := session.Execute(ctx, CreateUser("Alice"))
//	    if err != nil {
//	        return err // Automatic rollback
//	    }
//
//	    _, err = session.Execute(ctx, CreateProfile(user.ID))
//	    return err // Automatic commit if nil, rollback if error
//	})
func RunInTransaction[D, C, QF any](ctx context.Context, driver Driver[D, C, QF], fn func(session *SessionManaged[QF]) error, opts ...Option[C]) (err error) {
	session, err := driver.Transaction(ctx, opts...)
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

	managed, err := NewSessionManaged(session.backend)
	if err != nil {
		return err
	}

	err = fn(managed)
	if err != nil {
		return err
	}

	return session.Commit(ctx)
}

// Handler defines a function that uses a query factory to process database operations and returns typed results.
//
// Example:
//
//	func GetUserByID(id int) octobe.Handler[User, postgres.QueryFactory] {
//	    return func(ctx context.Context, newQuery postgres.QueryFactory) (User, error) {
//	        var user User
//	        query := newQuery(`SELECT id, name, email FROM users WHERE id = $1`)
//	        err := query.WithArgs(id).QueryRow(ctx, &user.ID, &user.Name, &user.Email)
//	        return user, err
//	    }
//	}
type Handler[R, T any] func(context.Context, T) (R, error)

// NoResultHandler defines a handler that uses a query factory to perform database operations without returning a result.
//
// Example:
//
//	func UpdateUser(id int, name string) octobe.NoResultHandler[postgres.QueryFactory] {
//	    return func(ctx context.Context, newQuery postgres.QueryFactory) error {
//	        query := newQuery(`UPDATE users SET name = $2 WHERE id = $1`)
//	        _, err := query.WithArgs(id, name).Exec(ctx)
//	        return err
//	    }
//	}
type NoResultHandler[T any] func(context.Context, T) error

// executeSequence runs a sequence of handlers in sequence, passing the same query factory to each.
func executeSequence[R, QF any](ctx context.Context, qf QF, handlers ...Handler[R, QF]) ([]R, error) {
	results := make([]R, 0, len(handlers))
	for i, handler := range handlers {
		result, err := handler(ctx, qf)
		if err != nil {
			return nil, fmt.Errorf("handler %d failed: %w", i, err)
		}
		results = append(results, result)
	}
	return results, nil
}
