package postgres

import (
	"github.com/Kansuler/octobe/v3"
	"github.com/jackc/pgx/v5"
)

type (
	PGXDriver     octobe.Driver[pgxConn, pgxConfig, Builder]
	PGXPoolDriver octobe.Driver[pgxpoolConn, pgxConfig, Builder]
)

// Builder constructs executable query segments from SQL strings.
type Builder func(query string) Segment

// PGXTxOptions configures transaction behavior and isolation levels.
type PGXTxOptions pgx.TxOptions

type pgxConfig struct {
	txOptions *PGXTxOptions
}

// WithPGXTxOptions configures transaction options for the session.
func WithPGXTxOptions(options PGXTxOptions) octobe.Option[pgxConfig] {
	return func(c *pgxConfig) {
		c.txOptions = &options
	}
}

// Segment represents a prepared query with arguments that can be executed once.
// Once executed, the segment becomes invalid and cannot be reused.
//
// The single-use nature prevents accidental query reuse and ensures predictable behavior.
// To execute the same query multiple times, create new segments each time.
//
// Method chaining example:
//
//	result, err := builder(`INSERT INTO users (name) VALUES ($1) RETURNING id`)
//	    .Arguments("Alice")
//	    .QueryRow(&userID)
//
// Multiple operations example:
//
//	// First query
//	err := builder(`UPDATE users SET name = $1 WHERE id = $2`)
//	    .Arguments("Alice", 123)
//	    .QueryRow()
//
//	// Second query (new segment required)
//	err = builder(`DELETE FROM sessions WHERE user_id = $1`)
//	    .Arguments(123)
//	    .Exec()
type Segment interface {
	Arguments(args ...any) Segment
	Exec() (ExecResult, error)
	QueryRow(dest ...any) error
	Query(cb func(Rows) error) error
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
