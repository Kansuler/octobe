package postgres

import (
	"context"

	"github.com/Kansuler/octobe/v4"
	"github.com/jackc/pgx/v5"
)

type (
	// PGXDriver is the driver interface returned by OpenPGX, OpenPGXWithParseConfigOptions, and OpenPGXWithConn.
	PGXDriver = octobe.Driver[PGXConn, Config, QueryFactory]

	// PGXPoolDriver is the driver interface returned by OpenPGXPool and OpenPGXWithPool.
	PGXPoolDriver = octobe.Driver[PGXPool, Config, QueryFactory]

	// PGXPoolOpenFunc opens a pooled PostgreSQL driver.
	PGXPoolOpenFunc = octobe.OpenFunc[PGXPool, Config, QueryFactory]

	// Option configures PostgreSQL driver behavior.
	Option = octobe.Option[Config]
)

// QueryFactory constructs executable statements from SQL strings.
type QueryFactory func(query string) Statement

// PGXTxOptions configures transaction behavior and isolation levels.
type PGXTxOptions pgx.TxOptions

// Config stores PostgreSQL driver options.
type Config struct {
	txOptions *PGXTxOptions
}

// WithPGXTxOptions configures transaction options for the session.
func WithPGXTxOptions(options PGXTxOptions) Option {
	return func(c *Config) {
		c.txOptions = &options
	}
}

// transactionOptions applies transaction options to the given options slice, ensuring a non-nil txOptions field.
func transactionOptions(opts []Option) []Option {
	txOpts := make([]Option, 0, len(opts)+1)
	txOpts = append(txOpts, opts...)
	txOpts = append(txOpts, func(c *Config) {
		if c.txOptions == nil {
			c.txOptions = &PGXTxOptions{}
		}
	})
	return txOpts
}

// Statement represents a single-use SQL statement with arguments.
// Once executed, the statement becomes invalid and cannot be reused.
//
// The single-use nature prevents accidental query reuse and ensures predictable behavior.
// To execute the same query multiple times, create new statements each time.
//
// Method chaining example:
//
//	err := newQuery(`INSERT INTO users (name) VALUES ($1) RETURNING id`)
//	    .WithArgs("Alice")
//	    .QueryRow(ctx, &userID)
//
// Multiple operations example:
//
//	// First query
//	err := newQuery(`UPDATE users SET name = $1 WHERE id = $2`)
//	    .WithArgs("Alice", 123)
//	    .QueryRow(ctx)
//
//	// Second query (new statement required)
//	_, err = newQuery(`DELETE FROM sessions WHERE user_id = $1`)
//	    .WithArgs(123)
//	    .Exec(ctx)
type Statement interface {
	WithArgs(args ...any) Statement
	Exec(ctx context.Context) (ExecResult, error)
	QueryRow(ctx context.Context, dest ...any) error
	Query(ctx context.Context, handleRows func(Rows) error) error
}

// ExecResult contains the outcome of an INSERT, UPDATE, or DELETE operation.
type ExecResult struct {
	RowsAffected int64
}

// Rows provides iteration over query result sets with pgx/database compatibility.
// Callers must check Err() after Next() returns false to detect premature termination.
type Rows interface {
	// Err returns any error encountered during iteration.
	// Only call after rows are closed or Next() returns false.
	Err() error

	// Next advances to the next row, returning false when no more rows exist.
	// Automatically closes rows when iteration completes.
	Next() bool

	// Scan copies column values from the current row into dest variables.
	// Must call Next() and verify it returned true before calling Scan.
	Scan(dest ...any) error
}

var _ Rows = (pgx.Rows)(nil)
