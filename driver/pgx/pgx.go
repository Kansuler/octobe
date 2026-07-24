package pgx

import (
	"context"
	"errors"

	"github.com/Kansuler/octobe/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrNoRows           = pgx.ErrNoRows
	ErrTooManyRows      = pgx.ErrTooManyRows
	ErrTxClosed         = pgx.ErrTxClosed
	ErrTxCommitRollback = pgx.ErrTxCommitRollback
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

// PGXConn defines the pgx connection methods used by the driver.
type PGXConn interface {
	Close(context.Context) error
	Prepare(context.Context, string, string) (*pgconn.StatementDescription, error)
	Deallocate(context.Context, string) error
	DeallocateAll(context.Context) error
	Ping(context.Context) error
	PgConn() *pgconn.PgConn
	Config() *pgx.ConnConfig
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
}

var _ PGXConn = &pgx.Conn{}

type pgxConn struct {
	conn PGXConn
}

var _ PGXDriver = &pgxConn{}

// OpenPGX creates a pgx connection driver from a DSN string.
func OpenPGX(ctx context.Context, dsn string) octobe.OpenFunc[PGXConn, Config, QueryFactory] {
	return func() (PGXDriver, error) {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}

		return &pgxConn{
			conn: conn,
		}, nil
	}
}

// ParseConfigOptions wraps pgconn parse configuration options.
type ParseConfigOptions struct {
	pgconn.ParseConfigOptions
}

// OpenPGXWithParseConfigOptions creates a pgx connection driver with custom parse options.
func OpenPGXWithParseConfigOptions(ctx context.Context, dsn string, options ParseConfigOptions) octobe.OpenFunc[PGXConn, Config, QueryFactory] {
	return func() (PGXDriver, error) {
		conn, err := pgx.ConnectWithOptions(ctx, dsn, pgx.ParseConfigOptions{ParseConfigOptions: options.ParseConfigOptions})
		if err != nil {
			return nil, err
		}

		return &pgxConn{
			conn: conn,
		}, nil
	}
}

// OpenPGXWithConn creates a driver from an existing pgx connection.
func OpenPGXWithConn(conn PGXConn) octobe.OpenFunc[PGXConn, Config, QueryFactory] {
	return func() (PGXDriver, error) {
		if conn == nil {
			return nil, errors.New("conn is nil")
		}

		return &pgxConn{
			conn: conn,
		}, nil
	}
}

// OpenSession opens a non-transactional session on the underlying pgx connection.
func (d *pgxConn) Session(ctx context.Context) (*octobe.Session[QueryFactory], error) {
	return octobe.NewSession(&pgxSession{
		d: d,
	})
}

// BeginTx starts a new transactional session.
func (d *pgxConn) Transaction(ctx context.Context, opts ...Option) (*octobe.SessionTransaction[QueryFactory], error) {
	var cfg Config
	for _, opt := range transactionOptions(opts) {
		opt(&cfg)
	}

	var pgxOpts pgx.TxOptions
	if cfg.txOptions != nil {
		pgxOpts = pgx.TxOptions{
			IsoLevel:       cfg.txOptions.IsoLevel,
			AccessMode:     cfg.txOptions.AccessMode,
			DeferrableMode: cfg.txOptions.DeferrableMode,
			BeginQuery:     cfg.txOptions.BeginQuery,
		}
	}

	tx, err := d.conn.BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, err
	}

	return octobe.NewTransaction(&pgxSession{
		cfg:       cfg,
		tx:        tx,
		committed: false,
		closed:    false,
	})
}

// Close closes the connection.
func (d *pgxConn) Close(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("connection is nil")
	}
	return d.conn.Close(ctx)
}

// Ping pings the connection.
func (d *pgxConn) Ping(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("connection is nil")
	}
	return d.conn.Ping(ctx)
}

// RunInTransaction runs fn in a transaction managed by Octobe.
func (d *pgxConn) RunInTransaction(ctx context.Context, fn func(session *octobe.SessionManaged[QueryFactory]) error, opts ...Option) (err error) {
	return octobe.RunInTransaction[PGXConn](ctx, d, fn, opts...)
}

// pgxSession implements octobe.Backend for a pgx connection or transaction.
// It is not safe for concurrent use.
type pgxSession struct {
	cfg       Config
	tx        pgx.Tx
	d         *pgxConn
	committed bool
	closed    bool
}

var _ octobe.Backend[QueryFactory] = &pgxSession{}

// Commit commits the transaction. Only works for transactional sessions.
func (s *pgxSession) Commit(ctx context.Context) error {
	if s.committed {
		return errors.New("cannot commit a session that has already been committed")
	}

	if s.cfg.txOptions == nil {
		return errors.New("cannot commit without transaction")
	}
	if s.closed {
		return errors.New("cannot commit a session that has already been closed")
	}
	err := s.tx.Commit(ctx)
	s.committed = true
	if err == nil {
		s.closed = true
	}
	return err
}

// Rollback rolls back the transaction. Only works for transactional sessions.
func (s *pgxSession) Rollback(ctx context.Context) error {
	if s.tx == nil {
		return errors.New("cannot rollback without transaction")
	}
	if s.closed {
		return nil
	}
	defer func() {
		s.closed = true
	}()
	return s.tx.Rollback(ctx)
}

// Close closes the session, rolling back if it is transactional and not committed.
func (s *pgxSession) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	if s.cfg.txOptions != nil {
		return s.Rollback(ctx)
	}
	s.closed = true
	return nil
}

// QueryFactory returns a query factory for this session.
func (s *pgxSession) QueryFactory() QueryFactory {
	return func(query string) Statement {
		return &pgxStatement{
			query:   query,
			args:    nil,
			used:    false,
			session: s,
		}
	}
}

// pgxStatement represents a single-use query with arguments and execution tracking.
type pgxStatement struct {
	query   string
	args    []any
	used    bool
	session *pgxSession
}

var _ Statement = &pgxStatement{}

func (s *pgxStatement) use() {
	s.used = true
}

// activeSession returns the session associated with this statement, or an error if it is closed.
func (s *pgxStatement) activeSession() (*pgxSession, error) {
	if s.session == nil || s.session.closed {
		return nil, errors.New("session is closed")
	}
	return s.session, nil
}

// WithArgs sets query parameters and returns the statement for method chaining.
func (s *pgxStatement) WithArgs(args ...any) Statement {
	s.args = args
	return s
}

// Exec executes the query and returns the number of affected rows.
func (s *pgxStatement) Exec(ctx context.Context) (ExecResult, error) {
	if s.used {
		return ExecResult{}, octobe.ErrStatementAlreadyExecuted
	}
	defer s.use()
	session, err := s.activeSession()
	if err != nil {
		return ExecResult{}, err
	}
	if session.tx == nil {
		res, err := session.d.conn.Exec(ctx, s.query, s.args...)
		if err != nil {
			return ExecResult{}, err
		}

		return ExecResult{
			RowsAffected: res.RowsAffected(),
		}, nil
	}

	res, err := session.tx.Exec(ctx, s.query, s.args...)
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{
		RowsAffected: res.RowsAffected(),
	}, nil
}

// QueryRow executes the query expecting exactly one row and scans into dest.
func (s *pgxStatement) QueryRow(ctx context.Context, dest ...any) error {
	if s.used {
		return octobe.ErrStatementAlreadyExecuted
	}
	defer s.use()
	session, err := s.activeSession()
	if err != nil {
		return err
	}
	if session.tx == nil {
		return session.d.conn.QueryRow(ctx, s.query, s.args...).Scan(dest...)
	}
	return session.tx.QueryRow(ctx, s.query, s.args...).Scan(dest...)
}

// Query executes the query and passes the result set to handleRows.
func (s *pgxStatement) Query(ctx context.Context, handleRows func(Rows) error) error {
	if s.used {
		return octobe.ErrStatementAlreadyExecuted
	}
	defer s.use()

	session, err := s.activeSession()
	if err != nil {
		return err
	}

	var rows pgx.Rows
	if session.tx == nil {
		rows, err = session.d.conn.Query(ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	} else {
		rows, err = session.tx.Query(ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	}

	defer rows.Close()
	if err = handleRows(rows); err != nil {
		return err
	}

	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}
