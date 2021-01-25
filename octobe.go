package octobe

import (
	"context"
	"database/sql"
	"errors"
)

// Head object that holds the database session
type Octobe struct {
	DB *sql.DB
}

// New initiates a DB instance and connection.
func New(db *sql.DB) Octobe {
	return Octobe{DB: db}
}

// ErrUsed is an error that emits if used is true on Segment.
var ErrUsed = errors.New("this query has already executed")

// Scheme holds context for the duration of the transaction
type Scheme struct {
	tx  *sql.Tx
	db  *sql.DB
	ctx context.Context
}

// Handler is a signature that can be used for handling
// database segments in a separate function
type Handler func(scheme Scheme) error

// Handle is a method that handle a handler
func (scheme Scheme) Handle(handler Handler) error {
	return handler(scheme)
}

// BeginTx initiates a transaction against database
func (ob Octobe) BeginTx(ctx context.Context, opts *sql.TxOptions) (scheme Scheme, err error) {
	scheme.tx, err = ob.DB.BeginTx(ctx, opts)
	scheme.db = ob.DB
	scheme.ctx = ctx
	return
}

// Begin initiates a query run, but not as a transaction
func (ob Octobe) Begin(ctx context.Context) (scheme Scheme) {
	scheme.db = ob.DB
	scheme.ctx = ctx
	return
}

// Segment is a specific query that can be run only once
// it keeps a few fields for keeping track on the segment
type Segment struct {
	// query in SQL that is going to be executed
	query string
	// args include argument values
	args []interface{}
	// used specify if this segment already has been executed
	used bool
	// tx is the database transaction, initiated by BeginTx
	tx *sql.Tx
	// db is the database instance
	db *sql.DB
	// ctx is a context that can be used to interrupt a query
	ctx context.Context
}

// use will set used to true after a segment has been performed
func (segment *Segment) use() {
	segment.used = true
}

// NewSegment created a new query within a database transaction
func (scheme *Scheme) NewSegment(query string) *Segment {
	return &Segment{
		query: query,
		args:  nil,
		tx:    scheme.tx,
		db:    scheme.db,
		ctx:   scheme.ctx,
	}
}

// Arguments receives unknown amount of arguments to use in the query
func (segment *Segment) Arguments(args ...interface{}) {
	segment.args = args
}

// Exec will execute a query. Used for inserts or updates
func (segment *Segment) Exec() error {
	if segment.used {
		return ErrUsed
	}

	defer segment.use()

	if segment.tx != nil {
		_, err := segment.tx.ExecContext(segment.ctx, segment.query, segment.args...)
		return err
	}

	_, err := segment.db.ExecContext(segment.ctx, segment.query, segment.args...)
	return err
}

// QueryRow will return one result and put them into destination pointers
func (segment *Segment) QueryRow(dest ...interface{}) error {
	if segment.used {
		return ErrUsed
	}

	defer segment.use()

	if segment.tx != nil {
		return segment.tx.QueryRowContext(segment.ctx, segment.query, segment.args...).Scan(dest...)
	}

	return segment.db.QueryRowContext(segment.ctx, segment.query, segment.args...).Scan(dest...)
}

// Query will perform a normal query against database that returns rows
func (segment *Segment) Query(cb func(*sql.Rows) error) error {
	if segment.used {
		return ErrUsed
	}

	defer segment.use()

	var rows *sql.Rows
	var err error
	if segment.tx != nil {
		rows, err = segment.tx.QueryContext(segment.ctx, segment.query, segment.args...)
	} else {
		rows, err = segment.db.QueryContext(segment.ctx, segment.query, segment.args...)
	}

	if err != nil {
		return err
	}

	err = cb(rows)
	if err != nil {
		_ = rows.Close()
		return err
	}

	return rows.Close()
}

// Insert will perform a query, and will also take destination pointers
// for returning data
func (segment *Segment) Insert(dest ...interface{}) error {
	if segment.used {
		return ErrUsed
	}

	defer segment.use()

	if segment.tx != nil {
		return segment.tx.QueryRowContext(segment.ctx, segment.query, segment.args...).Scan(dest...)
	}

	return segment.db.QueryRowContext(segment.ctx, segment.query, segment.args...).Scan(dest...)
}

// Commit will commit a transaction
func (scheme Scheme) Commit() error {
	return scheme.tx.Commit()
}

// Rollback will rollback a transaction
func (scheme Scheme) Rollback() error {
	return scheme.tx.Rollback()
}

// WatchRollback will perform a rollback if an error is given
// This method can be used as a defer in the function that performs
// the database operations.
func (scheme Scheme) WatchRollback(cb func() error) {
	if cb() != nil {
		_ = scheme.tx.Rollback()
	}
}

// WatchTransaction will perform the whole transaction, or do rollback if error occurred.
func (ob Octobe) WatchTransaction(ctx context.Context, cb func(scheme Scheme) error, opts ...*sql.TxOptions) error {
	scheme, err := ob.BeginTx(ctx, opts[0])
	if err != nil {
		return err
	}

	err = cb(scheme)

	if err != nil {
		return scheme.Rollback()
	}

	return scheme.Commit()
}
