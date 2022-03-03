package octobe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Octobe struct that holds the database session
type Octobe struct {
	// DB is the database instance
	DB *sql.DB
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
func New(db *sql.DB) Octobe {
	return Octobe{DB: db}
}

type octobeError struct {
	original error
	txError  error
}

func (err octobeError) Error() string {
	return fmt.Sprintf("%s, %s", err.original, err.txError)
}

func (err octobeError) Unwrap() error {
	return err.txError
}

func (err octobeError) Is(target error) bool {
	return errors.Is(err.original, target)
}

func (err octobeError) As(target interface{}) bool {
	return errors.As(err.original, target)
}

// ErrUsed is an error that emits if used is true on Segment.
var ErrUsed = errors.New("this query has already executed")

// ErrNeedInput is an error that require inputs for the inser method
var ErrNeedInput = errors.New("insert method require at least one argument")

// Scheme holds context for the duration of the operation
type Scheme struct {
	// tx is the database transaction, initiated by BeginTx
	tx *sql.Tx
	// db is the database instance
	db *sql.DB
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
	if len(opts) == 0 {
		opts = append(opts, &sql.TxOptions{})
	}

	scheme.tx, err = ob.DB.BeginTx(ctx, opts[0])
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

// Segment is a specific query that can be run only once it keeps a few fields for keeping track on the segment
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

// Segment created a new query segment
func (scheme *Scheme) Segment(query string) *Segment {
	return &Segment{
		query: query,
		args:  nil,
		tx:    nil,
		db:    scheme.db,
		ctx:   scheme.ctx,
	}
}

// Arguments receives unknown amount of arguments to use in the query
func (segment *Segment) Arguments(args ...interface{}) {
	segment.args = args
}

// Exec will execute a query. Used for inserts or updates
func (segment *Segment) Exec() (result sql.Result, err error) {
	if segment.used {
		return nil, ErrUsed
	}

	defer segment.use()

	if segment.tx != nil {
		result, err = segment.tx.ExecContext(segment.ctx, segment.query, segment.args...)
		return result, err
	}

	result, err = segment.db.ExecContext(segment.ctx, segment.query, segment.args...)
	return result, err
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
		return octobeError{
			original: err,
			txError:  rows.Close(),
		}
	}

	return rows.Close()
}

// Commit will commit a transaction
func (scheme *Scheme) Commit() error {
	if scheme.tx == nil {
		return nil
	}

	return scheme.tx.Commit()
}

// Rollback will rollback a transaction
func (scheme *Scheme) Rollback() error {
	if scheme.tx == nil {
		return nil
	}

	return scheme.tx.Rollback()
}

// WatchRollback will perform a rollback if an error is given
// This method can be used as a defer in the function that performs
// the database operations.
func (scheme *Scheme) WatchRollback(cb func() error) error {
	if scheme.tx == nil {
		return nil
	}

	if err := cb(); err != nil {
		return octobeError{
			original: err,
			txError:  scheme.Rollback(),
		}
	}
	return nil
}

// WatchTransaction will perform the whole transaction, or do rollback if error occurred.
func (ob Octobe) WatchTransaction(ctx context.Context, cb func(scheme *Scheme) error, opts ...*sql.TxOptions) error {
	if len(opts) == 0 {
		opts = append(opts, &sql.TxOptions{})
	}

	scheme, err := ob.BeginTx(ctx, opts[0])
	if err != nil {
		return err
	}

	err = cb(&scheme)

	if err != nil {
		return octobeError{
			original: err,
			txError:  scheme.Rollback(),
		}
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
