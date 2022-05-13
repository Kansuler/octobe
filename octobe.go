package octobe

import (
	"context"
	"database/sql"
	"errors"
)

type dblike interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type transactionlike interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// Octobe struct that holds the database session
type Octobe struct {
	// DB is the database instance
	DB dblike
}

// Option interface that tells what type of option it is
type Option interface {
	Type() string
}

// option is an internal struct for storing various options
type option struct {
	// suppressErrs will prevent these errors to surface, it could typically be sql.ErrNoRows that isn't a real error
	suppressErrs []error
}

// New initiates a DB instance and connection.
func New(db dblike) Octobe {
	return Octobe{DB: db}
}

// ErrUsed is an error that emits if used is true on Segment.
var ErrUsed = errors.New("this query has already executed")

// ErrNeedInput is an error that require inputs for the inser method
var ErrNeedInput = errors.New("insert method require at least one argument")

// Scheme holds context for the duration of the operation
type Scheme struct {
	db transactionlike
	// ctx is a context that can be used to interrupt a query
	ctx context.Context
}

// Handler is a signature that can be used for handling
// database segments in a separate function
type Handler func(scheme *Scheme) error

// Handle is a method that handle a handler
func (scheme *Scheme) Handle(handler Handler, opts ...Option) error {
	opt := convertOptions(opts...)
	return suppressErrors(handler(scheme), opt.suppressErrs)
}

// BeginTx initiates a transaction against database
func (ob Octobe) BeginTx(ctx context.Context, opts ...*sql.TxOptions) (scheme Scheme, err error) {
	var opt *sql.TxOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	scheme.db, err = ob.DB.BeginTx(ctx, opt)
	scheme.ctx = ctx
	return
}

// Begin initiates a query run, but not as a transaction
func (ob Octobe) Begin(ctx context.Context) (scheme Scheme) {
	scheme.db = ob.DB
	scheme.ctx = ctx
	return
}

// Segment is a specific query that can be run only once it keeps a few fields for keeping track on the segment
type Segment struct {
	// query in SQL that is going to be executed
	query string
	// args include argument values
	args []interface{}
	// used specify if this segment already has been executed
	used bool
	// tx is the database transaction, initiated by BeginTx
	db transactionlike
	// ctx is a context that can be used to interrupt a query
	ctx context.Context
}

// use will set used to true after a segment has been performed
func (segment *Segment) use() {
	segment.used = true
}

// Segment created a new query segment
func (scheme *Scheme) Segment(query string) *Segment {
	return &Segment{
		query: query,
		args:  nil,
		db:    scheme.db,
		ctx:   scheme.ctx,
	}
}

// Arguments receives unknown amount of arguments to use in the query
func (segment *Segment) Arguments(args ...interface{}) *Segment {
	segment.args = args
	return segment
}

// Exec will execute a query. Used for inserts or updates
func (segment *Segment) Exec() (result sql.Result, err error) {
	if segment.used {
		return nil, ErrUsed
	}

	defer segment.use()

	result, err = segment.db.ExecContext(segment.ctx, segment.query, segment.args...)
	return result, err
}

// QueryRow will return one result and put them into destination pointers
func (segment *Segment) QueryRow(dest ...interface{}) error {
	if segment.used {
		return ErrUsed
	}

	defer segment.use()

	return segment.db.QueryRowContext(segment.ctx, segment.query, segment.args...).Scan(dest...)
}

// Query will perform a normal query against database that returns rows
func (segment *Segment) Query(cb func(*sql.Rows) error) error {
	if segment.used {
		return ErrUsed
	}

	defer segment.use()

	rows, err := segment.db.QueryContext(segment.ctx, segment.query, segment.args...)
	if err != nil {
		return err
	}

	if err := cb(rows); err != nil {
		return errs{err, rows.Close()}
	}

	return rows.Close()
}

// Commit will commit a transaction
func (scheme *Scheme) Commit() error {
	if asCommit, ok := scheme.db.(interface{ Commit() error }); ok {
		return asCommit.Commit()
	}
	return nil
}

// Rollback will rollback a transaction
func (scheme *Scheme) Rollback() error {
	if asRollback, ok := scheme.db.(interface{ Rollback() error }); ok {
		return asRollback.Rollback()
	}
	return nil
}

// WatchRollback will perform a rollback if an error is given
// This method can be used as a defer in the function that performs
// the database operations.
func (scheme *Scheme) WatchRollback(cb func() error) error {
	if err := cb(); err != nil {
		return errs{err, scheme.Rollback()}
	}
	return nil
}

// WatchTransaction will perform the whole transaction, or do rollback if error occurred.
func (ob Octobe) WatchTransaction(ctx context.Context, cb func(scheme *Scheme) error, opts ...*sql.TxOptions) error {
	var opt *sql.TxOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	scheme, err := ob.BeginTx(ctx, opt)
	if err != nil {
		return err
	}

	err = cb(&scheme)

	if err != nil {
		return errs{err, scheme.Rollback()}
	}

	return scheme.Commit()
}

// suppressErrors is a helper function that suppress sql.ErrNoRows
func suppressErrors(err error, errs []error) error {
	for _, suppressErr := range errs {
		if err == suppressErr {
			return nil
		}
	}

	return err
}
